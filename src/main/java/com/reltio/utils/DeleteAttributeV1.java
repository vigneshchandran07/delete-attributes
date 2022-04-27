package com.reltio.utils;

import com.reltio.cst.exception.handler.GenericException;
import com.reltio.cst.exception.handler.ReltioAPICallFailureException;
import com.reltio.cst.service.ReltioAPIService;
import com.reltio.cst.util.Util;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Data
@Slf4j
public class DeleteAttributeV1 {
    public static void main(String[] args) throws Exception {

        long st = System.currentTimeMillis();


        log.info("Delete Attribute Program Started at {}", st);
        log.info("Reading Properties File..");
        Properties properties;


        properties = Util.getProperties(args[0], "PASSWORD");


        String env = properties.getProperty("ENVIRONMENT");
        String tenantID = properties.getProperty("TENANT_ID");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(properties.getProperty("FAILED_TO_DELETE")));
        BufferedWriter bufferedWriter2 = new BufferedWriter(new FileWriter(properties.getProperty("SUCCESS_TO_DELETE")));

        ReltioAPIService reltioAPIService = Util.getReltioService(properties);


        String url = "https://" +
                env +
                ".reltio.com/reltio/api/" +
                tenantID +
                "/" +
                "%entityID%" +
                "/_update?options=sendHidden,updateAttributeUpdateDates,addRefAttrUriToCrosswalk";


        int threads = Integer.parseInt(properties.getProperty("NO_OF_THREADS"));
        ExecutorService executorService = Executors.newFixedThreadPool(threads);

        long totalFuturesExecutionTime = 0L;
        List<Future<Long>> futures = new ArrayList<>();

        boolean eof = false;

        while (!eof) {
            try {
                Scanner scanner = new Scanner(new File(properties.getProperty("INPUT_FILE")));
                scanner.nextLine();

                while (scanner.hasNextLine()) {
                    String[] lines = scanner.nextLine().split(",");
                    String entityID = lines[0];
                    log.debug(entityID);
                    String finalUrl = url.replace("%entityID%", entityID);
                    String deleteBody =
                            "[{\"type\":\"DELETE_ATTRIBUTE\"," +
                                    "\"uri\":\"%URI%\"," +
                                    "\"crosswalk\":{\"type\":\"configuration/sources/%CROSSWALK_TYPE%\"," +
                                    "\"value\":\"%CROSSWALK_VALUE%\"," +
                                    "\"sourceTable\":\"%CROSSWALK_SOURCETABLE%\"}}]";

                    String fullUri = lines[3];
                    deleteBody = deleteBody
                            .replace("%URI%", fullUri)
                            .replace("%CROSSWALK_TYPE%", lines[5])
                            .replace("%CROSSWALK_VALUE%", lines[4])
                            .replace("%CROSSWALK_SOURCETABLE%", lines[1]);

                    String finalDeleteBody = deleteBody;
                    futures.add(executorService.submit(() -> delete(finalUrl, finalDeleteBody, reltioAPIService, bufferedWriter, bufferedWriter2, fullUri)));
                    //delete(finalUrl, finalDeleteBody, reltioAPIService);

                }
                eof = true;
                scanner.close();
            } catch (FileNotFoundException e) {
                log.error(e.getMessage());
            }

            waitForTasksReady(futures,
                    10);
        }
        waitForTasksReady(futures, 0);
        bufferedWriter.close();
        bufferedWriter2.close();
        executorService.shutdown();


    }

    private static Long delete(String url, String body, ReltioAPIService reltioAPIService, BufferedWriter bufferedWriter,
                               BufferedWriter successLogs, String fullUri) throws IOException {
        long st = System.currentTimeMillis();
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        try {
            String response = reltioAPIService.post(url, headers, body);
            log.debug(response);
            log.info("Currently Deleting.. {} ", fullUri);
            successLogs.write(fullUri + " : Deleted\n");
        } catch (ReltioAPICallFailureException e) {
            log.error("Reltio Error {}", Arrays.toString(e.getStackTrace()));
            bufferedWriter.write(url + " : " + e.getErrorResponse() + "\n");

        } catch (GenericException e) {
            log.error("Generic Error {}", Arrays.toString(e.getStackTrace()));
            bufferedWriter.write(url + " : " + e.getMessage() + "\n");

        }
        return System.currentTimeMillis() - st;
    }

    public static long waitForTasksReady(Collection<Future<Long>> futures,
                                         int maxNumberInList) {
        long totalResult = 0l;
        while (futures.size() > maxNumberInList) {
            try {
                Thread.sleep(20);
            } catch (Exception e) {
                // ignore it...
            }
            for (Future<Long> future : new ArrayList<Future<Long>>(futures)) {
                if (future.isDone()) {
                    try {
                        totalResult += future.get();
                        futures.remove(future);
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }
            }
        }
        return totalResult;
    }
}




/*
        POST : {{tenantURL}}/entities/{{entityID}}/_update?options=sendHidden,updateAttributeUpdateDates
        Body :
        [
            {
                "type": "DELETE_ATTRIBUTE",
                "uri": "entities/{{entityID}}/attributes/Identifiers//{{ Identifiers Value}}",
                "crosswalk": {
                    "type": "configuration/sources/Source",
                    "sourceTable": "Source",
                    "value": "CW Value"
                }
        ]
]
 */