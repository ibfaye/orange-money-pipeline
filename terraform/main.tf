# ═══════════════════════════════════════════════════════════════════
# Orange Money Pipeline — Terraform Configuration
# ═══════════════════════════════════════════════════════════════════
#
# Provisions Azure Databricks infrastructure for the Orange Money
# Medallion pipeline: workspace, Unity Catalog, job clusters,
# and Delta Lake storage.
#
# Usage:
#   cp terraform.tfvars.example terraform.tfvars
#   # Edit terraform.tfvars with your values
#   terraform init
#   terraform plan
#   terraform apply
#
# ═══════════════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.90.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.40.0"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }
}

# ═══════════════════════════════════════════════════════════════════
# Providers
# ═══════════════════════════════════════════════════════════════════

provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
  tenant_id       = var.azure_tenant_id
}

provider "databricks" {
  host  = azurerm_databricks_workspace.this.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}

# ═══════════════════════════════════════════════════════════════════
# Resource Group
# ═══════════════════════════════════════════════════════════════════

resource "azurerm_resource_group" "this" {
  name     = "${var.project_prefix}-rg"
  location = var.location

  tags = var.tags
}

# ═══════════════════════════════════════════════════════════════════
# Azure Data Lake Storage Gen2 (Delta Lake storage)
# ═══════════════════════════════════════════════════════════════════

resource "azurerm_storage_account" "datalake" {
  name                     = "${var.project_prefix}dl${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical namespace for ADLS Gen2

  tags = var.tags
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# ═══════════════════════════════════════════════════════════════════
# Azure Databricks Workspace
# ═══════════════════════════════════════════════════════════════════

resource "azurerm_databricks_workspace" "this" {
  name                = "${var.project_prefix}-databricks"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = var.databricks_sku

  tags = var.tags
}

# ═══════════════════════════════════════════════════════════════════
# Random suffix for globally unique names
# ═══════════════════════════════════════════════════════════════════

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
  numeric = true
}
