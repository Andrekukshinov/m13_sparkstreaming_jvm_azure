# Setup azurerm as a state backend
terraform {
  backend "azurerm" {
    resource_group_name  = "DataBricksHomeworks"
    storage_account_name = "fortf2"
    container_name       = "tfstate"
    key                  = "root.terraform.tfstate"
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}


provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.bdcc.id
}
data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "bdcc" {
  name = "rg-${var.ENV}-${var.LOCATION}"
  location = var.LOCATION

  lifecycle {
    prevent_destroy = false
  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_account" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc]

  name = "st${var.ENV}${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  account_tier = "Standard"
  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled = "true"

  network_rules {
    default_action = "Allow"
    ip_rules = values(var.IP_RULES)
  }

  lifecycle {
    prevent_destroy = false
  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
  depends_on = [
    azurerm_storage_account.bdcc]

  name = "data"
  storage_account_id = azurerm_storage_account.bdcc.id

  lifecycle {
    prevent_destroy = false
  }
}

resource "azurerm_storage_data_lake_gen2_path" "m13sparkstreaming" {
  path               = "m13sparkstreaming"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.gen2_data.name
  storage_account_id = azurerm_storage_account.bdcc.id
  resource           = "directory"

}

resource "azurerm_databricks_workspace" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc
  ]

  name = "dbw-${var.ENV}-${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  sku = "standard"

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}



#--------------------------------
data "databricks_node_type" "standard_f4" {
  depends_on    = [azurerm_resource_group.bdcc]
  local_disk    = true
  category      = "Compute Optimized"
  min_cores     = 4
  min_memory_gb = 8
}

data "databricks_spark_version" "latest_lts" {
  depends_on        = [azurerm_resource_group.bdcc]
  long_term_support = true
}

resource "databricks_cluster" "single_node" {
  cluster_name            = "running"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.standard_f4.id
  autotermination_minutes = 30
  spark_conf              = {
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*, 4]"
    "fs.azure.account.key.${azurerm_storage_account.bdcc.name}.dfs.core.windows.net" : azurerm_storage_account.bdcc.primary_access_key
    "stor.acc.name" : azurerm_storage_account.bdcc.name
    "contain.name" : azurerm_storage_data_lake_gen2_filesystem.gen2_data.name
  }
  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}

resource "databricks_notebook" "ddl" {
  path     = "/Notebooks/notebook"
  source   = "../notebooks/srcNotebook.scala"
  language = "SCALA"
}

resource "databricks_job" "this" {
  name                = "Job to run the only available notebook"
  existing_cluster_id = databricks_cluster.single_node.id

  notebook_task {
    notebook_path = databricks_notebook.ddl.path

  }
}