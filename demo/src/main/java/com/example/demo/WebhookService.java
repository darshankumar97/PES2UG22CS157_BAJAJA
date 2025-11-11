package com.example.demo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.json.JSONObject;

import java.nio.file.*;
import java.sql.*;
import java.util.*;

@Component
public class WebhookService implements CommandLineRunner {

    private static final String INITIAL_URL =
            "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

    @Override
    public void run(String... args) {
        RestTemplate restTemplate = new RestTemplate();

        try {
            // Step 1: Generate webhook URL and access token
            JSONObject body = new JSONObject();
            body.put("name", "Darshan Kumar K R");
            body.put("email", "pes2ug22cs157@pesu.pes.edu");
            body.put("regNo", "PES2UG22CS157");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> entity = new HttpEntity<>(body.toString(), headers);

            System.out.println("🚀 Sending first POST request...");
            ResponseEntity<String> response = restTemplate.postForEntity(INITIAL_URL, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                JSONObject responseBody = new JSONObject(response.getBody());
                String webhookUrl = responseBody.getString("webhook");
                String accessToken = responseBody.getString("accessToken").trim();

                System.out.println("🔗 Webhook URL: " + webhookUrl);
                System.out.println("🔑 Access Token: " + accessToken);

                // Step 2: Build final submission JSON
                String query =
                        "SELECT P.AMOUNT AS SALARY, " +
                        "CONCAT(E.FIRST_NAME, ' ', E.LAST_NAME) AS NAME, " +
                        "(YEAR(CURDATE()) - YEAR(E.DOB) - " +
                        "(CASE WHEN MONTH(CURDATE()) < MONTH(E.DOB) " +
                        "OR (MONTH(CURDATE()) = MONTH(E.DOB) AND DAY(CURDATE()) < DAY(E.DOB)) " +
                        "THEN 1 ELSE 0 END)) AS AGE, " +
                        "D.DEPARTMENT_NAME " +
                        "FROM PAYMENTS P " +
                        "JOIN EMPLOYEE E ON P.EMP_ID = E.EMP_ID " +
                        "JOIN DEPARTMENT D ON E.DEPARTMENT = D.DEPARTMENT_ID " +
                        "WHERE DAY(P.PAYMENT_TIME) != 1 " +
                        "ORDER BY P.AMOUNT DESC " +
                        "LIMIT 1;";

                JSONObject finalBody = new JSONObject();
                finalBody.put("query", query);

                // Step 3: Prepare headers as per required format
                HttpHeaders finalHeaders = new HttpHeaders();
                finalHeaders.setContentType(MediaType.APPLICATION_JSON);
                finalHeaders.set("Authorization", accessToken); // ⚠️ no Bearer

                HttpEntity<String> finalEntity = new HttpEntity<>(finalBody.toString(), finalHeaders);

                System.out.println("🧠 Final headers: " + finalHeaders);
                System.out.println("📝 Final body: " + finalBody.toString());

                // Step 4: Execute the query locally on data.sql
                executeLocalSQLQuery(query);

                // Step 5: Send the final POST request
                System.out.println("📡 Sending final POST request...");
                ResponseEntity<String> finalResponse = restTemplate.exchange(
                        webhookUrl,
                        HttpMethod.POST,
                        finalEntity,
                        String.class
                );

                if (finalResponse.getStatusCode().is2xxSuccessful()) {
                    System.out.println("✅ Final submission successful!");
                    System.out.println("📄 Response: " + finalResponse.getBody());
                } else {
                    System.out.println("❌ Final submission failed: " + finalResponse.getStatusCode());
                    System.out.println("🧾 Response body: " + finalResponse.getBody());
                }

            } else {
                System.out.println("❌ Initial request failed: " + response.getStatusCode());
                System.out.println("🧾 Response: " + response.getBody());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 💾 Executes the query locally on an in-memory H2 DB using data.sql
    private void executeLocalSQLQuery(String query) {
        System.out.println("\n🧩 Running query on local data.sql...");

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "sa", "")) {
            Statement stmt = conn.createStatement();

            // Load data.sql
            String sqlScript = Files.readString(Paths.get("src/main/resources/data.sql"));
            for (String statement : sqlScript.split(";")) {
                if (!statement.trim().isEmpty()) {
                    stmt.execute(statement);
                }
            }

            // Run your query
            ResultSet rs = stmt.executeQuery(query.replace("CURDATE()", "CURRENT_DATE"));

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            System.out.println("📊 Query Result:");
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + " | ");
                }
                System.out.println();
            }

        } catch (Exception e) {
            System.out.println("⚠️ Error executing local SQL: " + e.getMessage());
        }
    }
}
