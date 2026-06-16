# ═══════════════════════════════════════════════════════════════════
# Variables
# ═══════════════════════════════════════════════════════════════════

variable "project_prefix" {
  description = "Prefix for all Azure resources (e.g., 'om-pipeline')"
  type        = string
  default     = "orange-money"
}

variable "location" {
  description = "Azure region for deployment"
  type        = string
  default     = "francecentral"
}

variable "azure_subscription_id" {
  description = "Azure Subscription ID"
  type        = string
  sensitive   = true
}

variable "azure_tenant_id" {
  description = "Azure Tenant ID"
  type        = string
  sensitive   = true
}

variable "databricks_sku" {
  description = "Databricks workspace SKU (standard, premium, trial)"
  type        = string
  default     = "premium"

  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "SKU must be one of: standard, premium, trial"
  }
}

variable "environment" {
  description = "Environment: dev, staging, prod"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be: dev, staging, or prod"
  }
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default = {
    Project     = "Orange Money Pipeline"
    ManagedBy   = "Terraform"
    Environment = "dev"
    Owner       = "SenAnalytics"
  }
}
