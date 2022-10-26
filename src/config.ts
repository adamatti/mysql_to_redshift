import fs from 'fs';
import YAML from 'yaml';

const file = fs.readFileSync('./config.yaml', 'utf8');
export const config = YAML.parse(file);


