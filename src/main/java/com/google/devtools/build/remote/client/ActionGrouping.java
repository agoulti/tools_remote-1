package com.google.devtools.build.remote.client;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.devtools.build.lib.remote.logging.RemoteExecutionLog.ExecuteDetails;
import com.google.devtools.build.lib.remote.logging.RemoteExecutionLog.LogEntry;
import com.google.devtools.build.lib.remote.logging.RemoteExecutionLog.RpcCallDetails;
import com.google.longrunning.Operation;
import com.google.longrunning.Operation.ResultCase;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status.Code;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A class to handle GRPc log grouped by actions */
final class ActionGrouping {

  @VisibleForTesting
  static final String actionDelimiter = "************************************************";

  @VisibleForTesting
  static final String entryDelimiter = "------------------------------------------------";

  @VisibleForTesting static final String actionString = "Entries for action with hash '%s'\n";

  // Key: actionId; Value: a set of associated log entries.
  private Map<String, Multiset<LogEntry>> actionMap = new HashMap<>();

  // True if found V1 entries in the log.
  private boolean V1found = false;

  // The number of entries skipped
  private int numSkipped = 0;

  private boolean isV1Entry(RpcCallDetails details) {
    return details.hasV1Execute() || details.hasV1FindMissingBlobs() || details.hasV1GetActionResult() || details.hasV1Watch();
  }

  private Digest getDigest(LogEntry entry) {
    if(!entry.hasDetails()) {
      return null;
    }
    RpcCallDetails details = entry.getDetails();
    if(details.hasExecute()) {
      if(details.getExecute().hasRequest() && details.getExecute().getRequest().hasActionDigest()) {
        return details.getExecute().getRequest().getActionDigest();
      }
    }
    if(details.hasGetActionResult()) {
      if(details.getGetActionResult().hasRequest() && details.getGetActionResult().getRequest().hasActionDigest()) {
        return details.getGetActionResult().getRequest().getActionDigest();
      }
    }
    return null;
  }

  public void addLogEntry(LogEntry entry) {
    if(entry.hasDetails() && isV1Entry(entry.getDetails())) {
      V1found = true;
    }

    if (!entry.hasMetadata()) {
      numSkipped++;
      return;
    }
    String hash = entry.getMetadata().getActionId();

    if (!actionMap.containsKey(hash)) {
      actionMap.put(
          hash,
          TreeMultiset.create(
              (a, b) -> {
                int i = Timestamps.compare(a.getStartTime(), b.getStartTime());
                if (i != 0) {
                  return i;
                }
                // In the improbable case of the same timestamp, ensure the messages do not
                // override each other.
                return a.hashCode() - b.hashCode();
              }));
    }
    actionMap.get(hash).add(entry);
  }

  public void printByAction(PrintWriter out) throws IOException {
    for (String hash : actionMap.keySet()) {
      out.println(actionDelimiter);
      out.printf(actionString, hash);
      out.println(actionDelimiter);
      for (LogEntry entry : actionMap.get(hash)) {
        LogParserUtils.printLogEntry(entry, out);
        out.println(entryDelimiter);
      }
    }
    if (numSkipped > 0) {
      System.err.printf(
          "WARNING: Skipped %d entrie(s) due to absence of request metadata.\n", numSkipped);
    }
  }


  private class ResponseOrError {
    public String error;
    public ExecuteResponse response;

    public ResponseOrError(ExecuteResponse response) {
      this.response = response;
      this.error = "";
    }

    public ResponseOrError(String error) {
      this.response = null;
      this.error = error;
    }
  }

  private ResponseOrError getResponseOrError(List<Operation> operations) throws IOException {
    for(Operation o : operations) {
      StringBuilder error = new StringBuilder();
      ExecuteResponse response = LogParserUtils.getExecutionResponse(o, ExecuteResponse.class, error);
      if(response != null) {
        return new ResponseOrError(response);
      }
      String errorString = error.toString();
      if(!errorString.isEmpty()) {
        return new ResponseOrError(errorString);
      }
    }
    return null;
  }

  public List<Digest> failedActions() throws IOException {
    if(V1found) {
      System.err.printf(
          "This functinality is not supported for V1 API. Please upgrade your Bazel version.");
      System.exit(1);
    }

    int problematic = 0;
    ResponseOrError response = null; // Error or response from the last Execution
    ArrayList<Digest> result = new ArrayList<Digest>();

    for (String hash : actionMap.keySet()) {
      Digest digest = null;

      for (LogEntry entry : actionMap.get(hash)) {
        if(!entry.hasDetails()) {
          problematic++;
          continue;
        }
        Digest d = getDigest(entry);
        if(d != null) {
          if(digest != null && digest.equals(d)) {
            System.err.println("Inconsistent digests for action " + hash + " : " + d.getHash() + "/" + d.getSizeBytes() + " != " + digest.getHash() + "/" + digest.getSizeBytes());
          }
          digest = d;
        }

        RpcCallDetails details = entry.getDetails();

        if(details.hasExecute()) {
          ExecuteDetails execute = details.getExecute();

          if(entry.getStatus().getCode() != Code.OK.value()) {
            response = new ResponseOrError("\"Execution status code was \" + entry.getStatus().getCode()");
          } else {
            response = getResponseOrError(execute.getResponsesList());
          }
        } else if(details.hasWaitExecution()) {
          response = getResponseOrError(details.getWaitExecution().getResponsesList());
        }
      }
      if(response == null) {
        // No executions: gotten result from ActionCache or cache-only execution
        continue;
      }

      boolean failed = false;

      if (response.response == null) {
        failed = true;
      } else if (!response.response.hasResult()) {
        failed = true;
      } else {
        ActionResult a = response.response.getResult();
        if(a.getExitCode() != 0) {
          failed = true;
        }
      }

      if(failed) {
        // Last execution resulted in an error. Add this action to failed actions list
        if (digest == null) {
          System.err.println("Error: missing digest for action " + hash);
        } else {
          result.add(digest);
        }
        continue;
      }
    }
    if(problematic > 0) {
      System.err.printf("Skipped %d misformed entrie(s).\n", problematic);
    }
    return result;
  }
}
