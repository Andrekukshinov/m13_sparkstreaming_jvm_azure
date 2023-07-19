## This project is implemented in databricks with azure and to create infrastructure you should have at least 1 active azure acc with storage account created to store tf state remotely, go and edit main.tf: backend "azurerm"** with you storage acc data.
## And then you will be ready to run the app:
(First-time) Run from cmd
* make terraform-first-run

For all the subsequent runs:
* make terraform-first-run

### Then go to your created azure databricks account and run the job that was created for you


### State before incremental copy
![img.png](imgs/img.png) 
### Copying
![img_2.png](imgs/img_2.png)
### Streaming
![img_3.png](imgs/img_3.png)
### State after
![img_1.png](imgs/img_1.png)

### Query plans:
ten_biggest_cities_ordered
![img_4.png](imgs/img_4.png)
resulting table(containing all the cols for visualization)
![img_5.png](imgs/img_5.png)
![img_6.png](imgs/img_6.png)

### Paris
![img_7.png](imgs/img_7.png)
### London 
![img_8.png](imgs/img_8.png)
### Barcelona
![img_9.png](imgs/img_9.png)
### Milan
![img_10.png](imgs/img_10.png)
### Amsterdam
![img_11.png](imgs/img_11.png)
### Paddington
![img_12.png](imgs/img_12.png)
### San Diego
![img_13.png](imgs/img_13.png)
### New York
![img_14.png](imgs/img_14.png)
### Vienna
![img_15.png](imgs/img_15.png)
### Memphis
![img_16.png](imgs/img_16.png)
* Add your code in `src/main/` if needed
* Test your code with `src/tests/` if needed
* Modify notebooks for your needs
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch notebooks on Databricks cluster
