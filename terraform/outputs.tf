# ═══════════════════════════════════════════════════════════════════
# Outputs
# ═══════════════════════════════════════════════════════════════════

output "resource_group_name" {
  description = "Azure Resource Group name"
  value       = azurerm_resource_group.this.name
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_workspace_id" {
  description = "Databricks workspace resource ID"
  value       = azurerm_databricks_workspace.this.id
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = azurerm_storage_account.datalake.name
}

output "storage_account_key" {
  description = "ADLS Gen2 storage account key (sensitive)"
  value       = azurerm_storage_account.datalake.primary_access_key
  sensitive   = true
}

output "bronze_container" {
  description = "Bronze layer container name"
  value       = azurerm_storage_container.bronze.name
}

output "silver_container" {
  description = "Silver layer container name"
  value       = azurerm_storage_container.silver.name
}

output "gold_container" {
  description = "Gold layer container name"
  value       = azurerm_storage_container.gold.name
}
