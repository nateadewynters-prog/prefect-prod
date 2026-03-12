# 🗺️ High-Level System Architecture

This diagram illustrates the end-to-end data flow of the **Sales Report Extraction** pipeline, highlighting the recent transition to server-side state management, the new failure tagging, and the retry/reset mechanism.

```mermaid
graph TD
    %% 1. Trigger
    A[Prefect Cron Trigger] --> B[Load Rules from JSON]
    
    %% 2. Search & Filter
    B --> C{Subject-Only Keyword Search}
    C -->|Filter: No Tag Found| D[Identify Candidate Emails]
    D --> E[Python Sender Validation]
    C -->|Filter: Has 'extracted' or 'failed' Tag| F[Skip Already Handled]
    
    %% 3. Download
    E --> G[Download Attachment to Inbox]
    
    %% 4. Routing Logic
    G --> H{Passthrough Only?}
    
    %% 5a. Passthrough Path
    H -->|Yes| I[Move Raw File to Processed Zone]
    I --> J[Flush & Sync to Disk]
    
    %% 5b. Standard Path
    H -->|No| K[Invoke Dynamic Parser]
    K --> L[Validate Data Schema]
    L -->|Success| M[Save Standardized CSV to Processed Zone]
    M --> J
    
    %% 6. Delivery
    J --> N[Upload to SFTP Server]
    
    %% 7. Finalize State
    N --> O{Process Success?}
    O -->|Yes| P[Apply 'sales_report_extracted' Tag]
    O -->|No| Q[Apply 'sales_report_failed' Tag]
    
    %% 8. Alerts
    P --> R[Post Teams Success Alert]
    Q --> S[Move File to Failed Zone]
    S --> T[Post Teams Failure Alert]
    
    %% 9. Retry Loop
    U[UI: Reset Failed Emails] --> V[Untag 'sales_report_failed']
    V --> A
    
    %% Styling
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style P fill:#afa,stroke:#333,stroke-width:2px
    style Q fill:#faa,stroke:#333,stroke-width:2px
    style T fill:#faa,stroke:#333,stroke-width:2px
    style H fill:#fff,stroke:#333,stroke-width:4px
    style U fill:#ff9,stroke:#333,stroke-width:2px
```

## Key Architectural Highlights

- **Prefect Orchestration:** Manages the overall lifecycle, retries, and monitoring. Supports UI parameters (`days_back`, `target_rule_name`, `retry_failed`, `disable_notifications`) for historical backfills and bulk corrections.
- **Server-Side Idempotency:** The Microsoft Graph API acts as the state store via the `"sales_report_extracted"` and `"sales_report_failed"` category tags, ensuring each email is only processed once unless reset.
- **Dynamic Routing:** Supports both complex parsing (Standard Path) and simple file delivery (Passthrough Path) within the same engine.
- **Robust Search:** Employs a simplified, subject-only keyword search to bypass KQL query limitations, with sender validation handled purely in Python.
- **Stateless Operation:** Uses a 30-day dynamic rolling window instead of local persistence, ensuring high resilience to local storage failure.
- **Data Integrity:** Employs explicit OS-level flushing (`os.fsync`) before SFTP delivery to ensure zero-byte errors are avoided.
- **Observability:** Provides detailed validation artifacts and real-time Teams alerts for both successful and failed extractions, with a toggle to silence notifications during maintenance.
- **Bulk Retry Mechanism:** Explicit task (`reset_failed_emails`) to untag failed reports based on a time-window, enabling automated reprocessing of quarantine items.
