#!/usr/bin/env ts-node

import chalk from "chalk";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";

import { factory } from "./db";

const yarg = yargs(hideBin(process.argv));

yarg.usage("Usage: <subcommand>");
yarg.wrap(Math.min(130, yarg.terminalWidth()));

yarg.option("env", {
    describe: "name of env file to use for database creds",
    default: ".env.local",
    type: "string",
});

// yarg.parse();


console.log(yarg.argv)
require("dotenv").config({ path: yarg.argv['env'] });

factory().then(client => {
    const res = client.query('SELECT NOW()').then(x => {
        console.log("query", x)
        client.end().then(x => console.log("done", x))
    })
})
