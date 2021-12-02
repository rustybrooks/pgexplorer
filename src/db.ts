// require("dotenv").config({ path: env });

const { Client } = require('pg')

let client = null;

export async function factory() {
    if (client === null) {
        const client = new Client()
        await client.connect()
    }
}




