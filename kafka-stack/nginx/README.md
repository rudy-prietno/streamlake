### 🔧 Nginx Configuration

```bash
# Edit Nginx configs
sudo nano /etc/nginx/conf.d/kafka-connect.conf
sudo nano /etc/nginx/conf.d/schema-registry.conf

# Test configuration
sudo nginx -t

# Reload Nginx
sudo systemctl reload nginx

# Check status
sudo systemctl status nginx

# Move to configs folder
cd kafka-connect-configs

# Create connector
curl -s -X POST http://IP-EC2-NGINX:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @kafka_connector.json | jq
