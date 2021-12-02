// require("dotenv").config({ path: env });

const { Client } = require('pg')

let client = null;

export async function factory() {
    if (client === null) {
        console.log("connect")
        const client = new Client()
        await client.connect()
        console.log("done connect")
    }

    return client
}




