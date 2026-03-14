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
    E --> EE{Is internetMessageId Unique?}
    EE -->|No| EF[Apply 'sales_report_duplicate' Tag]
    EE -->|Yes| G[Download Attachment to Inbox]
    C -->|Filter: Has 'extracted' or 'failed' Tag| F[Skip Already Handled]
    
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
    J --> N[Upload to SFTP & SharePoint]
    
    %% 7. Finalize State
    N --> O{Process Success?}
    O -->|Yes| P[Apply 'sales_report_extracted' Tag]
    O -->|No| Q[Apply 'sales_report_failed' Tag]
    
    %% 8. Alerts
    P --> R[Post Truncated Batch Summary to Ops Channel]
    Q --> S[Move File to Failed Zone]
    S --> T[Post Real-Time Error Alert to Dev Channel]
    
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
    style EE fill:#fff,stroke:#333,stroke-width:4px
    style EF fill:#ff9,stroke:#333,stroke-width:2px
```

## Key Architectural Highlights

- **Dual-Channel Orchestration:** Alerts are routed based on target audience. **Operations** receive high-level, truncated batch summaries (Ops Channel), while **Developers** receive granular technical error details (Dev Channel).
- **Multi-Layer Idempotency:** The system uses both Microsoft Graph categories and `internetMessageId` fingerprinting. Duplicates are tagged as `"sales_report_duplicate"` and skipped, preventing redundant processing.
- **Server-Side State Management:** The Graph API acts as the state store via `"sales_report_extracted"`, `"sales_report_failed"`, and `"sales_report_duplicate"` tags.
- **Universal Logging:** The `get_universal_logger` utility ensures seamless logging whether the script is running in production (Prefect) or locally (Standard Python), facilitating safer development and testing.
- **Centralized Alerting:** Notifications have been removed from low-level utilities (SFTP, SharePoint) and moved to the orchestrator. Exceptions now bubble up, ensuring consistent reporting and fewer "noisy" partial failures.
- **Truncated Batch Reporting:** To keep communication channels clean, successful batch summaries are truncated (e.g., showing the first 10 items) with links to full records on SharePoint.
- **Dynamic Routing:** Supports both complex parsing (Standard Path) and simple file delivery (Passthrough Path) within the same engine.
- **Stateless Operation:** Uses a 30-day dynamic rolling window instead of local persistence, ensuring high resilience to local storage failure.
- **Data Integrity:** Employs explicit OS-level flushing (`os.fsync`) before SFTP delivery to ensure zero-byte errors are avoided.
- **Bulk Retry Mechanism:** Explicit task (`reset_failed_emails`) to untag failed reports based on a time-window, enabling automated reprocessing of quarantine items.
