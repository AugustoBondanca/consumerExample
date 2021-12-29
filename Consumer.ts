const common = require("oci-common");
const st = require("oci-streaming"); // OCI SDK package for OSS

const ociConfigFile = "<config_file_path>";
const ociProfileName = "<config_file_profile_name>";
const ociMessageEndpointForStream = "<stream_message_endpoint>"; // example value "https://cell-1.streaming.region.oci.oraclecloud.com"
const ociStreamOcid = "<stream_OCID>";

// provide authentication for OCI and OSS
const provider = new common.ConfigFileAuthenticationDetailsProvider(ociConfigFile, ociProfileName);
  
async function main() {
  // OSS client to produce and consume messages from a Stream in OSS
  const client = new st.StreamClient({ authenticationDetailsProvider: provider });

  client.endpoint = ociMessageEndpointForStream;

  // Use a cursor for getting messages; each getMessages call will return a next-cursor for iteration.
  // There are a couple kinds of cursors, we will use group cursors

  // Committed offsets are managed for the group, and partitions
  // are dynamically balanced amongst consumers in the group.

  console.log("Starting a simple message loop with a group cursor");
  const groupCursor = await getCursorByGroup(client, ociStreamOcid, "exampleGroup01000", "exampleInstance-1");
  await simpleMessageLoop(client, ociStreamOcid, groupCursor);

}

async function getCursorByGroup(client, streamId, groupName, instanceName) {
    console.log("Creating a cursor for group %s, instance %s.", groupName, instanceName);
    const cursorDetails = {
      groupName: groupName,
      instanceName: instanceName,
      type: st.models.CreateGroupCursorDetails.Type.TrimHorizon,
      commitOnGet: true
    };
    const createCursorRequest = {
      createGroupCursorDetails: cursorDetails,
      streamId: streamId
    };
    const response = await client.createGroupCursor(createCursorRequest);
    return response.cursor.value;
  }

async function simpleMessageLoop(client, streamId, initialCursor) {
    let cursor = initialCursor;
    for (var i = 0; i < 5; i++) {
      const getRequest = {
        streamId: streamId,
        cursor: cursor,
        limit: 100
      };
      const response = await client.getMessages(getRequest);
      console.log("Read %s messages.", response.items.length);
      for (var message of response.items) { 
        if (message.key !== null)  {         
            console.log("Key: %s, Value: %s, Partition: %s",
            Buffer.from(message.key, "base64").toString(),
            Buffer.from(message.value, "base64").toString(),
            Buffer.from(message.partition, "utf8").toString());
        }
       else{
            console.log("Key: Null, Value: %s, Partition: %s",
                Buffer.from(message.value, "base64").toString(),
                Buffer.from(message.partition, "utf8").toString());
       }
      }
      
      // getMessages is a throttled method; clients should retrieve sufficiently large message
      // batches, as to avoid too many http requests.
      await delay(2);
      cursor = response.opcNextCursor;
    }
  }

  async function delay(s) {
    return new Promise(resolve => setTimeout(resolve, s * 1000));
  }

main().catch((err) => {
    console.log("Error occurred: ", err);
});
