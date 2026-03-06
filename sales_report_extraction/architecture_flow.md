# 🗺️ High-Level System Architecture

This diagram illustrates the end-to-end data flow of the **Sales Report Extraction** pipeline, highlighting the recent transition to server-side state management and the new passthrough feature.

```mermaid
graph TD
    %% 1. Trigger
    A[Prefect Cron Trigger] --> B[Load Rules from JSON]
    
    %% 2. Search & Filter
    B --> C{Search MS Graph API}
    C -->|Filter: No Tag Found| D[Identify Candidate Emails]
    C -->|Filter: Has Tag| E[Skip Already Processed]
    
    %% 3. Download
    D --> F[Download Attachment to Inbox]
    
    %% 4. Routing Logic
    F --> G{Passthrough Only?}
    
    %% 5a. Passthrough Path
    G -->|Yes| H[Move Raw File to Processed Zone]
    H --> I[Flush & Sync to Disk]
    
    %% 5b. Standard Path
    G -->|No| J[Invoke Dynamic Parser]
    J --> K[Validate Data Schema]
    K -->|Success| L[Save Standardized CSV to Processed Zone]
    L --> I
    
    %% 6. Delivery
    I --> M[Upload to SFTP Server]
    
    %% 7. Finalize State
    M --> N{Process Success?}
    N -->|Yes| O[Apply 'sales_report_extracted' Tag]
    N -->|No| P[Apply 'sales_report_extracted' Tag]
    
    %% 8. Alerts
    O --> Q[Post Teams Success Alert]
    P --> R[Move File to Failed Zone]
    R --> S[Post Teams Failure Alert]
    
    %% Styling
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style O fill:#afa,stroke:#333,stroke-width:2px
    style S fill:#faa,stroke:#333,stroke-width:2px
    style G fill:#fff,stroke:#333,stroke-width:4px
```

## Key Architectural Highlights

- **Prefect Orchestration:** Manages the overall lifecycle, retries, and monitoring.
- **Server-Side Idempotency:** The Microsoft Graph API acts as the state store via the `"sales_report_extracted"` category tag, ensuring each email is only processed once.
- **Dynamic Routing:** Supports both complex parsing (Standard Path) and simple file delivery (Passthrough Path) within the same engine.
- **Data Integrity:** Employs explicit OS-level flushing (`os.fsync`) before SFTP delivery to ensure zero-byte errors are avoided.
- **Observability:** Provides detailed validation artifacts and real-time Teams alerts for both successful and failed extractions.
