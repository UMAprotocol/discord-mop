import _axios, { Axios, AxiosResponse, AxiosResponseHeaders, RawAxiosRequestHeaders } from "axios";
import dotenv from "dotenv";
import bluebird from "bluebird";

dotenv.config();

const axios = new _axios.Axios({
    baseURL: "https://discord.com/api",
    headers: {
        Authorization: `Bot ${process.env.DISCORD_TOKEN}`,
        'Content-Type': 'application/json; charset=UTF-8',
        'User-Agent': 'Mop Bot (https://github.com/UMAprotocol/discord-mop, 1.0.0)',
    }
})

const { GUILD_ID, RETENTION_SECONDS, NAME_FILTER, ID_FILTER, DELETE, ID_EXCLUDE_FILTER } = process.env;

type Channel = {
    id: string;
    name?: string;
}

type Message = {
    id: string;
    channel_id: string;
    timestamp: string;
    date: Date;
}

type RateLimitResponse = {
    global: boolean;
    message: string;
    retry_after: number;
}

const rateLimitCache: { [id: string]: number } = {};

function delay(seconds: number) {
    return new Promise((resolve) => setTimeout(resolve, seconds * 1000));
}

async function getChannels(): Promise<Channel[]> {
    const { data, status } = await axios.get(`/guilds/${GUILD_ID}/channels`);
    if (status !== 200) throw new Error(`getChannelIds request failed with status ${status} ${data}`);
    return JSON.parse(data);
}

async function getActiveThreads(): Promise<Channel[]> {
    const { data, status } = await axios.get(`/guilds/${GUILD_ID}/threads/active`);
    if (status !== 200) throw new Error(`getActiveThreads request failed with status ${status} ${data}`);
    return JSON.parse(data).threads;
}

function recordLocalRateLimit(headers: RawAxiosRequestHeaders | AxiosResponseHeaders): string {
    if (!headers["x-ratelimit-remaining"]) throw new Error("No x-ratelimit-remaining header found");
    const rateLimitRemaining = Number(headers["x-ratelimit-remaining"]);
    const rateLimitBucket = headers["x-ratelimit-bucket"] as string | undefined;
    if (!rateLimitBucket) throw new Error(`x-ratelimit-bucket undefined.`);

    if (rateLimitRemaining === 0) {
        const resetRawValue = headers["x-ratelimit-reset"];
        if (!resetRawValue) throw new Error(`x-ratelimit-reset undefined.`);

        const rateLimitReset = Number(headers["x-ratelimit-reset"]);
        console.log(`Resetting rate limit to ${rateLimitReset - (Date.now() / 1000)} for bucket ${rateLimitBucket}. Is not global.`)
        rateLimitCache[rateLimitBucket] = rateLimitReset;
    }

    return rateLimitBucket;
}

function getRateLimitDelay(bucket: string | undefined): number {
    const currentTs = Date.now() / 1000;
    if (bucket !== undefined && rateLimitCache[bucket] !== undefined && currentTs < rateLimitCache[bucket]) {
        return rateLimitCache[bucket] - currentTs;
    }

    if (rateLimitCache["global"] !== undefined && currentTs < rateLimitCache["global"]) {
        return rateLimitCache["global"] - currentTs;
    }

    return 0;
}

function checkResponse(response: AxiosResponse, expectedStatus: number): { needsRetry: boolean; detectedBucket: string | undefined } {
    const { status, data, headers } = response;

    switch (status) {
        case 429:
            const rateLimitResponse = JSON.parse(data) as RateLimitResponse;
            console.log(`Rate limited. Setting reset times and retrying.`);
            if (rateLimitResponse.global) {
                // Global rate limit.
                console.log(`Rate limit is global. Retrying after ${rateLimitResponse.retry_after / 1000} seconds.`);
                rateLimitCache["global"] = (Date.now() + rateLimitResponse.retry_after) / 1000;
                return { needsRetry: true, detectedBucket: undefined }; // No bucket can be detected for global rate limits.
            } else {
                return { needsRetry: true, detectedBucket: recordLocalRateLimit(headers) };
            }
        case expectedStatus:
            return { needsRetry: false, detectedBucket: recordLocalRateLimit(headers) };
        default:
            throw new Error(`Received unexpected status ${status} ${data} ${headers}`);
    }
}

async function getAllMessagesInChannel(channelId: string) {
    const messages: Message[] = [];
    let before: undefined | string = undefined;
    let rateLimitBucket: undefined | string = undefined;
    for (;;) {

        const rateLimitDelay = getRateLimitDelay(rateLimitBucket);
        if (rateLimitDelay > 0) {
            await delay(rateLimitDelay);
            continue;
        }

        const response = await axios.get(`/channels/${channelId}/messages?limit=100${before ? `&before=${before}` : ""}`);

        const { needsRetry, detectedBucket } = checkResponse(response, 200);
        if (detectedBucket) rateLimitBucket = detectedBucket;
        if (needsRetry) continue;

        const newMessages: Message[] = (JSON.parse(response.data) as Omit<Message, "date">[]).map((data) => ({ ...data, date: new Date(data.timestamp) })).sort((a, b) => a.date.valueOf() - b.date.valueOf());

        if (newMessages.length === 0) return messages;

        messages.unshift(...newMessages);

        before = newMessages[0].id;
    }
}

let deleteRateLimitBucket: string | undefined = undefined;

async function deleteMessage(message: Message) {
    for (;;) {
        const rateLimitDelay = getRateLimitDelay(deleteRateLimitBucket);

        if (rateLimitDelay > 0) {
            await delay(rateLimitDelay);
            continue;
        }

        const response = await axios.delete(`/channels/${message.channel_id}/messages/${message.id}`);

        const { needsRetry, detectedBucket } = checkResponse(response, 204);

        if (detectedBucket) deleteRateLimitBucket = detectedBucket;
        if (needsRetry) continue;

        console.log(`Deleted message with id ${message.id} at date: ${message.date.toLocaleString()}`)
        break;
    }
}


async function main() {

    if (DELETE) {
        console.log("DELETE env specified, this script will delete messages on the discord server!");
    } else {
        console.log("DELETE env not specified, this script will perform a dry-run.");
    }

    console.log("Requesting Channels and Active Threads...");
    let channels = [...await getChannels(), ...await getActiveThreads()];

    if (NAME_FILTER) {
        console.log(`Filtering channels to only consider those with ${NAME_FILTER} in their name...`);
        channels = channels.filter(({ name }) => name?.includes(NAME_FILTER));
    }

    if (ID_FILTER) {
        console.log(`Filtering channels to only consider those with ids in ${ID_FILTER}...`);
        const ids = ID_FILTER.split(",").map(id => id.trim());
        channels = channels.filter(({ id }) => ids.includes(id));
    }

    if (ID_EXCLUDE_FILTER) {
        console.log(`Filtering channels to only remove those with ids in ${ID_EXCLUDE_FILTER}...`);
        const ids = ID_EXCLUDE_FILTER.split(",").map(id => id.trim());
        channels = channels.filter(({ id }) => !ids.includes(id));
    }

    console.log(`Got ${channels.length} channels`);
    console.log("Requesting messages from all channels...");

    const channelIdMapping = Object.fromEntries(channels.map(channel => [channel.id, channel]));

    const channelMessages = await bluebird.map(channels, async (channel, i) => {
        console.log(`Retrieving messages for channel ${i + 1}/${channels.length} with name ${channel.name}...`);
        const messages = await getAllMessagesInChannel(channel.id);
        console.log(`Retrieved ${messages.length} messages for ${channel.name}. Continuing...`);
        return messages;
    }, { concurrency: 5 });

    const messages = channelMessages.flat().sort((a, b) => a.date.valueOf() - b.date.valueOf());
    console.log(`Retrieved ${messages.length} messages`);

    const currentTime = Date.now();
    const retentionSeconds = Number(RETENTION_SECONDS);
    const deletedMessages = messages.filter(message => (currentTime - message.date.valueOf()) > (retentionSeconds * 1000));
    const retainedMessages = messages.filter(message => (currentTime - message.date.valueOf()) <= (retentionSeconds * 1000));
    console.log(`Retaining ${retainedMessages.length}, deleting ${deletedMessages.length}.`);

    if (retainedMessages.length > 0) console.log(`Oldest retained message: ${JSON.stringify(retainedMessages[0])} on channel ${channelIdMapping[retainedMessages[0].channel_id].name}`);
    if (deletedMessages.length > 0) console.log(`Latest deleted message: ${JSON.stringify(deletedMessages[deletedMessages.length - 1])} on channel ${channelIdMapping[deletedMessages[deletedMessages.length - 1].channel_id].name}`);

    if (!DELETE) {
        console.log("DELETE env variable not specified, so returning early.");
        return;
    }

    await bluebird.map(deletedMessages, deleteMessage, { concurrency: 1 });
}

main();