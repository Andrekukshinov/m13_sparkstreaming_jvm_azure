terraform-first-run:
		az login && \
		cd terraform && \
		terraform init && \
		terraform apply -auto-approve

terraform-subs-run:
		cd .\terraform && \
		terraform apply -auto-approve