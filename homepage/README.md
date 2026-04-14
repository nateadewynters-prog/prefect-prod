# 🏠 Service Homepage Hub

**Status:** Microservice (UI Tool)  
**Web Server:** Nginx Alpine  
**External Port:** `80`  

---

## 1. Overview

The **Service Homepage** acts as the central entry point for all internal reporting and data tools. It provides a clean, unified dashboard for navigating between the various microservices hosted on this server.

### Key Features

- **Lightweight Hub:** Built on `nginx:alpine` for minimal resource consumption.
- **Dynamic Routing:** Uses simple JavaScript to redirect users based on the current hostname and specific service ports.
- **Responsive UI:** Styled with Tailwind CSS for a modern, mobile-friendly experience.
- **Quick Links:** Immediate access to:
  - **🔍 DocID Tool (8003):** SQL Reference mapping.
  - **📊 Sales Dispatcher (8002):** PBI Automation tool.
  - **🧠 Prefect Server (4200):** Orchestration dashboard.
  - **🐳 Portainer (9000):** Container management.

---

## 2. Technical Stack

- **Nginx:** High-performance, low-memory static file serving.
- **Frontend:** Vanilla HTML5, Tailwind CSS (via CDN), and JavaScript.
- **Deployment:** Docker (Alpine Linux).

---

## 3. Configuration & Customization

### Adding New Services
To add a new service to the hub:
1. Edit `homepage/index.html`.
2. Add a new `div` card following the existing pattern.
3. Use the `gotoService('PORT_NUMBER')` function for the click handler.

### Style Modifications
The page utilizes Tailwind CSS utility classes. Significant changes to layout can be made directly in `index.html`.

---

## 4. Deployment

The service is managed via the root `docker-compose.yml`.

### Port Mapping
- **Container Port:** `80`
- **Host Port:** `80` (Standard HTTP)

### Restarting
```bash
docker-compose up -d --build homepage
```

---

## 5. Troubleshooting

### Page Not Loading
If the homepage is inaccessible:
1. Check if the container is running: `docker ps | grep homepage`
2. Ensure port 80 isn't being blocked by another service on the host.
3. Review Nginx logs: `docker logs -f homepage`
