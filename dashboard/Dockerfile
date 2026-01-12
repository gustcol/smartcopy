# SmartCopy Dashboard - Docker Image
#
# IMPORTANT: This Dockerfile is for the DASHBOARD ONLY.
# Do NOT run SmartCopy itself inside Docker as it will significantly
# impact I/O performance. The dashboard is designed for large-scale
# HPC environments where monitoring visibility is critical.
#
# Build: docker build -t smartcopy-dashboard .
# Run:   docker run -p 3000:80 -e API_URL=http://smartcopy-host:8080 smartcopy-dashboard

# Stage 1: Build
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY package.json package-lock.json* ./

# Install dependencies
RUN npm ci

# Copy source files
COPY . .

# Build the application
RUN npm run build

# Stage 2: Production
FROM nginx:alpine

# Copy nginx configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Copy built assets from builder stage
COPY --from=builder /app/dist /usr/share/nginx/html

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost/health || exit 1

# Expose port
EXPOSE 80

# Labels
LABEL org.opencontainers.image.title="SmartCopy Dashboard"
LABEL org.opencontainers.image.description="Monitoring dashboard for SmartCopy HPC file transfers"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.authors="SmartCopy Team"
LABEL org.opencontainers.image.source="https://github.com/smartcopy/smartcopy"

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
