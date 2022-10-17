import _axios, { Axios, AxiosResponseHeaders, RawAxiosRequestHeaders } from "axios";
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

const { GUILD_ID, RETENTION_SECONDS, NAME_FILTER, ID_FILTER, DELETE } = process.env;

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

async function getAllMessagesInChannel(channelId: string) {
    const messages: Message[] = [];
    let before: undefined | string = undefined;
    let rateLimitId: undefined | string = undefined;
    for (;;) {

        const currentTs = Date.now() / 1000;
        if (rateLimitId !== undefined && rateLimitCache[rateLimitId] !== undefined && currentTs < rateLimitCache[rateLimitId]) {
            await delay(rateLimitCache[rateLimitId] - currentTs);
            // After the delay, we should do this check again, so repeat the loop.
            continue;
        }

        if (rateLimitCache["global"] !== undefined && currentTs < rateLimitCache["global"]) {
            await delay(rateLimitCache["global"] - currentTs);
            continue;
        }

        const { data, status, headers } = await axios.get(`/channels/${channelId}/messages?limit=100${before ? `&before=${before}` : ""}`);

        // Permission denied and no messages yet found.
        if ((status === 401 || status === 403) && messages.length === 0) {
            console.log(`Got error when trying to retrieve messages. Status: ${status}, ${data}`);
            return messages;
        }

        if (status === 429) {
            const rateLimitResponse = JSON.parse(data) as RateLimitResponse;

            console.log(`Rate limited. Setting reset times and retrying.`);

            if (rateLimitResponse.global) {
                // Global rate limit.
                console.log(`Rate limit is global. Retrying after ${rateLimitResponse.retry_after / 1000} seconds.`);
                rateLimitCache["global"] = (Date.now() + rateLimitResponse.retry_after) / 1000;
            } else {
                rateLimitId = recordLocalRateLimit(headers);
            }
            continue;
        }

        rateLimitId = recordLocalRateLimit(headers);
        
        // Request failed for some other reason.
        if (status !== 200) throw new Error(`getAllMessagesInChannel(${channelId}) request failed with status ${status} ${data}`);

        const newMessages: Message[] = (JSON.parse(data) as Omit<Message, "date">[]).map((data) => ({ ...data, date: new Date(data.timestamp) })).sort((a, b) => a.date.valueOf() - b.date.valueOf());

        if (newMessages.length === 0) return messages;

        messages.unshift(...newMessages);

        before = newMessages[0].id;
    }
}

let rateLimitBucket: string | undefined = undefined;

async function deleteMessage(message: Message) {
    for (;;) {
        const currentTs = Date.now() / 1000;
        if (rateLimitCache["global"] !== undefined && currentTs < rateLimitCache["global"]) {
            await delay(rateLimitCache["global"] - currentTs);
            continue;
        }

        if (rateLimitBucket !== undefined && rateLimitCache[rateLimitBucket] !== undefined && currentTs < rateLimitCache[rateLimitBucket]) {
            await delay(rateLimitCache[rateLimitBucket] - currentTs);
            // After the delay, we should do this check again, so repeat the loop.
            continue;
        }
        const { status, headers, data } = await axios.delete(`/channels/${message.channel_id}/messages/${message.id}`);

        if (status === 429 && JSON.parse(data).global === true) {
            const rateLimitResponse = JSON.parse(data) as RateLimitResponse;
            // Global rate limit.
            console.log(`Rate limit is global. Retrying after ${rateLimitResponse.retry_after / 1000} seconds.`);
            rateLimitCache["global"] = (Date.now() + rateLimitResponse.retry_after) / 1000;
            continue;
        }

        if (status !== 204 && status !== 429) throw new Error(`Unrecognized Error ${status} ${headers} ${data}`);

        rateLimitBucket = recordLocalRateLimit(headers);

        if (status === 204) return;
    }
}


async function main() {

    if (DELETE) {
        console.log("DELETE env specified, this script will delete messages on the discord server!");
    } else {
        console.log("DELETE env not specified, this script will perform a dry-run.");
    }

    console.log("Requesting Channels...");
    let channels = await getChannels();
    if (NAME_FILTER) {
        console.log(`Filtering channels to only consider those with ${NAME_FILTER} in their name...`);
        channels = channels.filter(({ name }) => name?.includes(NAME_FILTER));
    }

    if (ID_FILTER) {
        console.log(`Filtering channels to only consider those with ids in ${ID_FILTER}...`);
        const ids = ID_FILTER.split(",").map(id => id.trim());
        channels = channels.filter(({ id }) => ids.includes(id));
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

    await bluebird.map(deletedMessages, deleteMessage, { concurrency: 5 });
}

main();