import _axios, { AxiosResponse, AxiosResponseHeaders, RawAxiosRequestHeaders } from "axios";
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

enum ChannelType {
    GUILD_TEXT = 0,
    DM = 1,
    GUILD_VOICE = 2,
    GROUP_DM = 3,
    GUILD_CATEGORY = 4,
    GUILD_ANNOUNCEMENT = 5,
    ANNOUNCEMENT_THREAD = 10,
    PUBLIC_THREAD = 11,
    PRIVATE_THREAD = 12,
    GUILD_STAGE_VOICE = 13,
    GUILD_DIRECTORY = 14,
    GUILD_FORUM = 15,
}

type Channel = {
    id: string;
    name?: string;
    type: ChannelType;
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
    if (!headers["x-ratelimit-remaining"] || !headers["x-ratelimit-bucket"]) {
        // console.log("WARN: rate limit headers missing");
        return "none";
    }
    const rateLimitRemaining = Number(headers["x-ratelimit-remaining"]);
    const rateLimitBucket = headers["x-ratelimit-bucket"] as string;

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

let archivedThreadsRateLimitBucket: string | undefined = undefined;

type Thread = {
    id: string,
    name?: string,
    type: ChannelType,
    thread_metadata: {
        archive_timestamp: string;
    }
}

type ArchivedThreadResponse = {
    threads: Thread[];
    has_more: boolean;
}

const publicExclusionList = new Set([ChannelType.GUILD_CATEGORY, ChannelType.GUILD_VOICE, ChannelType.PUBLIC_THREAD, ChannelType.PRIVATE_THREAD, ChannelType.GUILD_STAGE_VOICE]);
const privateExclusionList = new Set([ChannelType.GUILD_CATEGORY, ChannelType.GUILD_VOICE, ChannelType.PUBLIC_THREAD, ChannelType.PRIVATE_THREAD, ChannelType.GUILD_STAGE_VOICE, ChannelType.GUILD_ANNOUNCEMENT]);

async function getArchivedThreads(channel: Channel, type: "public" | "private"): Promise<Thread[]> {
    const threads: Thread[] = [];

    if (type === "public" && publicExclusionList.has(channel.type)) return [];
    else if (type === "private" && privateExclusionList.has(channel.type)) return [];

    let before: undefined | string = undefined;
    for (;;) {

        const rateLimitDelay = getRateLimitDelay(archivedThreadsRateLimitBucket);
        if (rateLimitDelay > 0) {
            await delay(rateLimitDelay);
            continue;
        }

        const response = await axios.get(`/channels/${channel.id}/threads/archived/${type}?limit=100&${before ? `&before=${before}` : ""}`);

        const { needsRetry, detectedBucket } = checkResponse(response, 200);
        if (detectedBucket) archivedThreadsRateLimitBucket = detectedBucket;
        if (needsRetry) continue;

        const responseData: ArchivedThreadResponse  = JSON.parse(response.data);

        threads.push(...responseData.threads);

        if (responseData.has_more) {
            before = new Date(responseData.threads[responseData.threads.length - 1].thread_metadata.archive_timestamp).toISOString();
            continue;
        }

        break;
    }

    if (threads.length > 0) console.log(channel.name, threads.length)
    return threads;
}

let deleteThreadRateLimitBucket: string | undefined = undefined;

async function deleteThread(thread: Thread) {
    for (;;) {
        const rateLimitDelay = getRateLimitDelay(deleteRateLimitBucket);

        if (rateLimitDelay > 0) {
            await delay(rateLimitDelay);
            continue;
        }

        const response = await axios.delete(`/channels/${thread.id}`);

        const { needsRetry, detectedBucket } = checkResponse(response, 200);

        if (detectedBucket) deleteThreadRateLimitBucket = detectedBucket;
        if (needsRetry) continue;

        console.log(`Deleted thread with id ${thread.id} and archive date: ${new Date(thread.thread_metadata.archive_timestamp).toLocaleString()}`);
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

    // Get all archived threads from each channel.
    const archivedThreads: Thread[] = [];
    for (const channel of channels) archivedThreads.push(...await getArchivedThreads(channel, "public"));
    for (const channel of channels) archivedThreads.push(...await getArchivedThreads(channel, "private"));

    console.log(`Got ${channels.length} channels and ${archivedThreads.length} archived threads`);
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
    const deletedThreads = archivedThreads.filter(thread => (currentTime - (new Date(thread.thread_metadata.archive_timestamp).valueOf())) > (retentionSeconds * 1000));
    const retainedThreads = archivedThreads.filter(thread => (currentTime - (new Date(thread.thread_metadata.archive_timestamp).valueOf())) <= (retentionSeconds * 1000));
    console.log(`Retaining ${retainedMessages.length} messages, deleting ${deletedMessages.length} messages.`);
    console.log(`Retaining ${retainedThreads.length} threads, deleting ${deletedThreads.length} threads`);

    if (retainedMessages.length > 0) console.log(`Oldest retained message: ${JSON.stringify(retainedMessages[0])} on channel ${channelIdMapping[retainedMessages[0].channel_id].name}`);
    if (deletedMessages.length > 0) console.log(`Latest deleted message: ${JSON.stringify(deletedMessages[deletedMessages.length - 1])} on channel ${channelIdMapping[deletedMessages[deletedMessages.length - 1].channel_id].name}`);

    if (!DELETE) {
        console.log("DELETE env variable not specified, so returning early.");
        return;
    }

    for (const thread of deletedThreads) await deleteThread(thread);
    for (const message of deletedMessages) await deleteMessage(message);
}

main();