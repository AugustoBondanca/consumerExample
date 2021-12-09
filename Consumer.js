var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var common = require("oci-common");
var st = require("oci-streaming"); // OCI SDK package for OSS
var ociConfigFile = "/home/augusto_bo/.oci/config";
var ociProfileName = "DEFAULT";
var ociMessageEndpointForStream = "https://streaming.us-phoenix-1.oci.oraclecloud.com"; // example value "https://cell-1.streaming.region.oci.oraclecloud.com"
var ociStreamOcid = "ocid1.stream.oc1.phx.amaaaaaaeun4owyam7r2t25lrxrk5ajelylyle2eh64we4osguac4rwyl5ma";
// provide authentication for OCI and OSS
var provider = new common.ConfigFileAuthenticationDetailsProvider(ociConfigFile, ociProfileName);
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var client, groupCursor;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    client = new st.StreamClient({ authenticationDetailsProvider: provider });
                    client.endpoint = ociMessageEndpointForStream;
                    // Use a cursor for getting messages; each getMessages call will return a next-cursor for iteration.
                    // There are a couple kinds of cursors, we will use group cursors
                    // Committed offsets are managed for the group, and partitions
                    // are dynamically balanced amongst consumers in the group.
                    console.log("Starting a simple message loop with a group cursor");
                    return [4 /*yield*/, getCursorByGroup(client, ociStreamOcid, "exampleGroup01000", "exampleInstance-1")];
                case 1:
                    groupCursor = _a.sent();
                    return [4 /*yield*/, simpleMessageLoop(client, ociStreamOcid, groupCursor)];
                case 2:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function getCursorByGroup(client, streamId, groupName, instanceName) {
    return __awaiter(this, void 0, void 0, function () {
        var cursorDetails, createCursorRequest, response;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    console.log("Creating a cursor for group %s, instance %s.", groupName, instanceName);
                    cursorDetails = {
                        groupName: groupName,
                        instanceName: instanceName,
                        type: st.models.CreateGroupCursorDetails.Type.TrimHorizon,
                        commitOnGet: true
                    };
                    createCursorRequest = {
                        createGroupCursorDetails: cursorDetails,
                        streamId: streamId
                    };
                    return [4 /*yield*/, client.createGroupCursor(createCursorRequest)];
                case 1:
                    response = _a.sent();
                    return [2 /*return*/, response.cursor.value];
            }
        });
    });
}
function simpleMessageLoop(client, streamId, initialCursor) {
    return __awaiter(this, void 0, void 0, function () {
        var cursor, i, getRequest, response, _i, _a, message;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    cursor = initialCursor;
                    i = 0;
                    _b.label = 1;
                case 1:
                    if (!(i < 5)) return [3 /*break*/, 5];
                    getRequest = {
                        streamId: streamId,
                        cursor: cursor,
                        limit: 100
                    };
                    return [4 /*yield*/, client.getMessages(getRequest)];
                case 2:
                    response = _b.sent();
                    console.log("Read %s messages.", response.items.length);
                    for (_i = 0, _a = response.items; _i < _a.length; _i++) {
                        message = _a[_i];
                        if (message.key !== null) {
                            console.log("Key: %s, Value: %s, Partition: %s", Buffer.from(message.key, "base64").toString(), Buffer.from(message.value, "base64").toString(), Buffer.from(message.partition, "utf8").toString());
                        }
                        else {
                            console.log("Key: Null, Value: %s, Partition: %s", Buffer.from(message.value, "base64").toString(), Buffer.from(message.partition, "utf8").toString());
                        }
                    }
                    // getMessages is a throttled method; clients should retrieve sufficiently large message
                    // batches, as to avoid too many http requests.
                    return [4 /*yield*/, delay(2)];
                case 3:
                    // getMessages is a throttled method; clients should retrieve sufficiently large message
                    // batches, as to avoid too many http requests.
                    _b.sent();
                    cursor = response.opcNextCursor;
                    _b.label = 4;
                case 4:
                    i++;
                    return [3 /*break*/, 1];
                case 5: return [2 /*return*/];
            }
        });
    });
}
function delay(s) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve) { return setTimeout(resolve, s * 1000); })];
        });
    });
}
main()["catch"](function (err) {
    console.log("Error occurred: ", err);
});
