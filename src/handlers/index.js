const Route53 = require('aws-sdk/clients/route53');
const STS = require('aws-sdk/clients/sts');
const EC2 = require('aws-sdk/clients/ec2');
const _ = require('lodash');

const ROLE_SESSION_NAME = 'route53-zone-association';
const TAG = 'route53zones'

/**
 * A Lambda function that logs the payload received from a CloudWatch scheduled event.
 */
exports.handler = async (event, context) => {
    // All log statements are written to CloudWatch by default. For more information, see
    // https://docs.aws.amazon.com/lambda/latest/dg/nodejs-prog-model-logging.html
    console.info(JSON.stringify(event));

    const roleArn = event.roleArn;
    const region = event.region;

    var route53Owner = new Route53();

    var zones = await getAllZones(route53Owner);
    var private = _.filter(zones, { Config: { PrivateZone: true } });
    var tagged = await getTagsForZones(route53Owner, private);

    var memberCredentials = await getCredentials(roleArn);
    var ec2 = new EC2({ credentials: memberCredentials, region: region });
    var route53Member = new Route53({ credentials: memberCredentials });

    let params = {
        Filters: [{
            Name: 'tag-key',
            Values: [TAG]
        }]
    }
    console.debug('getVpcs: Params %O', params);
    var res = await ec2.describeVpcs(params).promise();
    console.debug('getVpcs: Response %O', res);

    for (let vpc of res.Vpcs) {
        console.debug('Updating associations for %O', vpc)
        await updateVpcAssociations(route53Owner, route53Member, vpc, region, tagged)
    }

    return tagged;
}

async function getAllZones(client) {
    console.debug('getAllZones: Requesting all Route 53 zones');

    let params = {
        MaxItems: '100'
    };
    let zones = [], res;

    do {
        console.debug('getAllZones: Params %O', params);
        res = await client.listHostedZones(params).promise();
        console.debug('getAllZones: Response %O', res);
        zones = zones.concat(_.map(res.HostedZones, zone => {
            return _.assign({}, zone, { Id: getZoneId(zone.Id) })
        }));
        params.Marker = res.NextMarker
    } while (res.IsTruncated)

    console.debug('getAllZones: Return %O', zones)
    return zones;
}

async function getTagsForZones(client, zones) {
    console.debug('getTagsForZones: Requesting tagged clones of Route 53 zones %j', zones);
    let zoneIds = _.map(zones, 'Id');
    let zoneChunks = _.chunk(zoneIds, 10);
    let tagged = [];

    for (const zoneChunk of zoneChunks) {
        let params = {
            ResourceIds: zoneChunk,
            ResourceType: 'hostedzone'
        }
        console.debug('getTagsForZones: Params %O', params);
        let res = await client.listTagsForResources(params).promise();
        console.debug('getTagsForZones: Response %O', res);
        let taggedChunks = _.map(res.ResourceTagSets, tagSet => {
            let zone = _.find(zones, ['Id', tagSet.ResourceId])
            return _.assign({}, zone, { Tags: getTagsObject(tagSet.Tags) })
        });
        tagged = tagged.concat(taggedChunks)
    };

    console.debug('getAllZones: Return %O', tagged)
    return tagged;
}

function getMatchingZones(zones, filters) {
    console.debug('getMatchingZones: Zones %O', zones);
    let matches = []

    for (const filter of filters) {
        console.debug('getMatchingZones: Filter %O', filter);
        let filterMatches = _.filter(zones, filter);
        console.debug('getMatchingZones: Matches %O', filterMatches);
        matches = matches.concat(filterMatches)
    }

    return _.uniq(matches);
}

async function getCredentials(role) {
    let sts = new STS();
    let params = {
        RoleArn: role,
        RoleSessionName: ROLE_SESSION_NAME
    };

    console.debug('getCredentials: Params %O', params);
    let res = await sts.assumeRole(params).promise();
    console.debug('getCredentials: Response %O', res);
    let credentials = _.mapKeys(res.Credentials, (value, key) => _.camelCase(key));
    console.debug('getCredentials: Credentials %O', credentials);

    return credentials
}

async function getAllHostedZonesByVpc(client, vpcId, vpcRegion) {
    console.debug('getAllHostedZonesByVpc: Requesting all Route 53 zones by VPC for %s in %s', vpcId, vpcRegion);

    let params = {
        MaxItems: '100',
        VPCId: vpcId,
        VPCRegion: vpcRegion
    };
    let hostedZoneSummaries = [], res;

    do {
        console.debug('getAllHostedZonesByVpc: Params %O', params);
        res = await client.listHostedZonesByVPC(params).promise();
        console.debug('getAllHostedZonesByVpc: Response %O', res);
        hostedZoneSummaries = hostedZoneSummaries.concat(res.HostedZoneSummaries);
        params.NextToken = res.NextToken
    } while (res.NextToken)

    console.debug('getAllHostedZonesByVpc: Return %O', hostedZoneSummaries)
    return hostedZoneSummaries;
}

async function updateVpcAssociations(ownerClient, memberClient, vpc, region, tagged) {
    console.debug('updateVpcAssociations: Updating associations for %O', vpc)
    try {
        let tags = getTagsObject(vpc.Tags)
        let filter = JSON.parse(tags[TAG]);
        console.debug('updateVpcAssociations: Filter zones: %O', filter);
        var matching = getMatchingZones(tagged, filter);
        let expectedZoneIds = _.map(matching, 'Id');
        console.debug('updateVpcAssociations: Expected Zone Ids: %j', expectedZoneIds);

        let res = await getAllHostedZonesByVpc(memberClient, vpc.VpcId, region);
        let actualZoneIds = _.map(res, 'HostedZoneId');
        console.debug('updateVpcAssociations: Actual Zone Ids: %j', actualZoneIds);

        let addZoneIds = _.difference(expectedZoneIds, actualZoneIds);
        console.debug('updateVpcAssociations: Add Zone Ids: %j', addZoneIds);
        for (const addZoneId of addZoneIds) {
            await associateVpcWithHostedZone(ownerClient, memberClient, addZoneId, vpc.VpcId, region)
        }

        let removeZoneIds = _.difference(actualZoneIds, expectedZoneIds);
        console.debug('updateVpcAssociations: Remove Zone Ids: %j', removeZoneIds);
        for (const removeZoneId of removeZoneIds) {
            await disassociateVpcFromHostedZone(ownerClient, memberClient, removeZoneId, vpc.VpcId, region)
        }

    } catch (err) {
        console.error('updateVpcAssociations: Error %O', err);
    }
}

async function associateVpcWithHostedZone(ownerClient, memberClient, zoneId, vpcId, vpcRegion) {
    console.debug('associateVpcWithHostedZone: %s %s %s', zoneId, vpcId, vpcRegion);
    let params = {
        HostedZoneId: zoneId,
        VPC: {
            VPCId: vpcId,
            VPCRegion: vpcRegion
        }
    };
    await ownerClient.createVPCAssociationAuthorization(params).promise();
    await memberClient.associateVPCWithHostedZone(params).promise();
    await ownerClient.deleteVPCAssociationAuthorization(params).promise();
}

async function disassociateVpcFromHostedZone(ownerClient, memberClient, zoneId, vpcId, vpcRegion) {
    console.debug('disassociateVpcFromHostedZone: %s %s %s', zoneId, vpcId, vpcRegion);
    let params = {
        HostedZoneId: zoneId,
        VPC: {
            VPCId: vpcId,
            VPCRegion: vpcRegion
        }
    };
    await memberClient.disassociateVPCFromHostedZone(params).promise();
}

function getZoneId(id) {
    return id.substring(12)
}

function getTagsObject(tags) {
    return _.zipObject(_.map(tags, 'Key'), _.map(tags, 'Value'));
}