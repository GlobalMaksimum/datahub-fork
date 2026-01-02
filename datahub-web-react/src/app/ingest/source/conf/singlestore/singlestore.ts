import { SourceConfig } from '@app/ingest/source/conf/types';

import singlestoreLogo from '@images/singlestorelogo.png';

const placeholderRecipe = `\
source: 
    type: singlestore
    config: 
        # Coordinates
        host_port: # Your SingleStore host and post, e.g. singlestore:3306
        database: # Your SingleStore database name, e.g. datahub
    
        # Credentials
        # Add secret in Secrets Tab with relevant names for each variable
        username: "\${SINGLESTORE_USERNAME}" # Your SingleStore username, e.g. admin
        password: "\${SINGLESTORE_PASSWORD}" # Your SingleStore password, e.g. password_01

        # Options
        include_tables: True
        include_views: True

        # Profiling
        profiling:
            enabled: false
`;

const singlestoreConfig: SourceConfig = {
    type: 'singlestore',
    placeholderRecipe,
    displayName: 'SingleStore',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/mysql/',
    logoUrl: singlestoreLogo,
};

export default singlestoreConfig;