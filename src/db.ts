import { Client } from 'pg';

let client = null;

export async function factory() {
    if (client === null) {
        console.log('connect');
        client = new Client();
        await client.connect();
        console.log('done connect');
    }
    console.log('retclient', client);
    return client;
}
