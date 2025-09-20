sudo nano /etc/nginx/conf.d/kafka-connect.conf
sudo nano /etc/nginx/conf.d/schema-registry.conf 
sudo nginx -t
sudo systemctl reload nginx
sudo systemctl status nginx
