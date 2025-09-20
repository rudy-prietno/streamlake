## bash command
sudo nano /etc/nginx/conf.d/kafka-connect.conf
sudo nano /etc/nginx/conf.d/schema-registry.conf
sudo nginx -t
sudo systemctl reload nginx
sudo systemctl status nginx

cd kafka-connect-configs
curl -s -X POST http://IP-EC2-NGINX:8083/connectors \
-H 'Content-Type: application/json' \
-d @kafka_connector.json | jq
