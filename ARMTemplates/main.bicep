param nameprefix string

@description('Location for all resources.')
param location string = resourceGroup().location
param skuTier string
param skuCapacity int
param isAutoInflateEnabled bool
param maximumThroughputUnits int
param zoneRedundant bool
param tags object

@description('Storage account SKU name.')
param storageAccountType string = 'Standard_LRS'

@description('Specifies whether Azure Virtual Machines are permitted to retrieve certificates stored as secrets from the key vault.')
param enabledForDeployment bool = false

@description('Specifies whether Azure Disk Encryption is permitted to retrieve secrets from the vault and unwrap keys.')
param enabledForDiskEncryption bool = false

@description('Specifies whether Azure Resource Manager is permitted to retrieve secrets from the key vault.')
param enabledForTemplateDeployment bool = false

@description('Specifies the Azure Active Directory tenant ID that should be used for authenticating requests to the key vault. Get it by using Get-AzSubscription cmdlet.')
param tenantId string = subscription().tenantId

@description('Specifies the permissions to keys in the vault. Valid values are: all, encrypt, decrypt, wrapKey, unwrapKey, sign, verify, get, list, create, update, import, delete, backup, restore, recover, and purge.')
param keysPermissions array = [
  'list'
]

@description('Specifies the permissions to secrets in the vault. Valid values are: all, get, list, set, delete, backup, restore, recover, and purge.')
param secretsPermissions array = [
  'list'
  'get'
  'set'
]

@description('Specifies whether the key vault is a standard vault or a premium vault.')
@allowed([
  'standard'
  'premium'
])
param skuName string = 'standard'

// param secretsObject object
param adtDomainName string

@description('Tenant ID of the App Registration that will be used to access Cognite Data Fusion. Store the client secret in the keyvault.')
param cdfTenantId string = subscription().tenantId

@description('CDF cluster.')
@allowed([
  'api'
  'westeurope-1'
  'asia-northeast1-1'
  'az-eastus-1'
  'az-power-no-northeurope'
])
param cdfCluster string

@description('Cognite project name')
param cdfProject string

@description('Root Asset of ADT resources created in CDF')
param rootAssetExternalID string = 'adt_root'

var adtName = '${nameprefix}-ADT-${uniqueString(resourceGroup().id)}'
var adtRoute = '${adtName}-route'
var managedIdentity = '${nameprefix}-managedIdentity-${uniqueString(resourceGroup().id)}'
var functionAppNameCDF2ADT = '${nameprefix}-FunctionCDF2ADT-${uniqueString(resourceGroup().id)}'
var functionAppNameADT2CDF = '${nameprefix}-FunctionADT2CDF-${uniqueString(resourceGroup().id)}'
var appRegistrationName = '${nameprefix}-CDFACCESS-${uniqueString(resourceGroup().id)}'
var hostingPlanNameADT2CDF = '${nameprefix}-AppServiceADT2CDF-${uniqueString(resourceGroup().id)}'
var hostingPlanNameCDF2ADT = '${nameprefix}-AppServiceCDF2ADT-${uniqueString(resourceGroup().id)}'
var applicationInsightsName = '${nameprefix}-AppInsights-${uniqueString(resourceGroup().id)}'
var keyVaultName = '${nameprefix}-kv-${uniqueString(resourceGroup().id)}'
var storageAccountName = toLower('${nameprefix}${uniqueString(resourceGroup().id)}')
var functionWorkerRuntime = 'python'
var eventHubName = 'adtsync'
var eventHubNamespaceName = '${nameprefix}-eventHubName-${uniqueString(resourceGroup().id)}'

var ADTroleDefinitionId = resourceId('Microsoft.Authorization/roleDefinitions', 'bcd981a7-7f74-457b-83e1-cceb9e632ffe')
var ADTroleDefinitionName = guid(identity.id, ADTroleDefinitionId, resourceGroup().id)
var ContributorRoleDefinitionId = resourceId('Microsoft.Authorization/roleDefinitions', 'b24988ac-6180-42a0-ab88-20f7382dd24c')
var ContributorRoleDefinitionName = guid(identity.id, ContributorRoleDefinitionId, resourceGroup().id)




resource eventHubNamespace 'Microsoft.EventHub/namespaces@2021-06-01-preview' = {
  name: eventHubNamespaceName
  location: location
  tags: tags
  sku: {
    name: skuTier
    tier: skuTier
    capacity: skuCapacity
  }
  properties: {
    isAutoInflateEnabled: isAutoInflateEnabled
    maximumThroughputUnits: maximumThroughputUnits
    zoneRedundant: zoneRedundant
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2021-11-01' = {
  parent: eventHubNamespace
  name: eventHubName
  properties: {
    messageRetentionInDays: 1
    partitionCount: 1
  }
}

resource authorization_send_listen 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2021-11-01' = {
  parent: eventHub
  name: 'send_listen'
  properties: {
    rights: [
      'Send'
      'Listen'
    ]
  }
}

param currentTime string = utcNow()

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2022-01-31-preview' = {
  name: managedIdentity
  location: location
}

// add "Digital Twins Data Owner" role to managed identity
resource adtroledef 'Microsoft.Authorization/roleAssignments@2018-09-01-preview' = {
  name: ADTroleDefinitionName
  properties: {
    roleDefinitionId: ADTroleDefinitionId
    principalId: reference(managedIdentity).principalId
    principalType: 'ServicePrincipal'
  }
}

// add "Contributor" role to managed identity
resource contribroledef 'Microsoft.Authorization/roleAssignments@2018-09-01-preview' = {
  name: ContributorRoleDefinitionName
  properties: {
    roleDefinitionId: ContributorRoleDefinitionId
    principalId: reference(managedIdentity).principalId
    principalType: 'ServicePrincipal'
  }
}


resource script 'Microsoft.Resources/deploymentScripts@2019-10-01-preview' = {
  name: appRegistrationName
  location: location
  kind: 'AzurePowerShell'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    azPowerShellVersion: '5.0'
    arguments: '-resourceName "${appRegistrationName}"'
    scriptContent: '''
      param([string] $resourceName)
      $token = (Get-AzAccessToken -ResourceUrl https://graph.microsoft.com).Token
      $headers = @{'Content-Type' = 'application/json'; 'Authorization' = 'Bearer ' + $token}
      $template = @{
        displayName = $resourceName
        requiredResourceAccess = @(
          @{
            resourceAppId = "00000003-0000-0000-c000-000000000000"
            resourceAccess = @(
              @{
                id = "e1fe6dd8-ba31-4d61-89e7-88639da4683d"
                type = "Scope"
              }
            )
          }
        )
        signInAudience = "AzureADMyOrg"
      }
      
      $app = (Invoke-RestMethod -Method Get -Headers $headers -Uri "https://graph.microsoft.com/beta/applications?filter=displayName eq '$($resourceName)'").value
      $principal = @{}
      if ($app) {
        $ignore = Invoke-RestMethod -Method Patch -Headers $headers -Uri "https://graph.microsoft.com/beta/applications/$($app.id)" -Body ($template | ConvertTo-Json -Depth 10)
        $principal = (Invoke-RestMethod -Method Get -Headers $headers -Uri "https://graph.microsoft.com/beta/servicePrincipals?filter=appId eq '$($app.appId)'").value
      } else {
        $app = (Invoke-RestMethod -Method Post -Headers $headers -Uri "https://graph.microsoft.com/beta/applications" -Body ($template | ConvertTo-Json -Depth 10))
        $principal = Invoke-RestMethod -Method POST -Headers $headers -Uri  "https://graph.microsoft.com/beta/servicePrincipals" -Body (@{ "appId" = $app.appId } | ConvertTo-Json)
      }
      
      $app = (Invoke-RestMethod -Method Get -Headers $headers -Uri "https://graph.microsoft.com/beta/applications/$($app.id)")
      
      foreach ($password in $app.passwordCredentials) {
        Write-Host "Deleting secret with id: $($password.keyId)"
        $body = @{
          "keyId" = $password.keyId
        }
        $ignore = Invoke-RestMethod -Method POST -Headers $headers -Uri "https://graph.microsoft.com/beta/applications/$($app.id)/removePassword" -Body ($body | ConvertTo-Json)
      }
      
      $body = @{
        "passwordCredential" = @{
          "displayName"= "Client Secret"
        }
      }
      $secret = (Invoke-RestMethod -Method POST -Headers $headers -Uri  "https://graph.microsoft.com/beta/applications/$($app.id)/addPassword" -Body ($body | ConvertTo-Json)).secretText
      
      $DeploymentScriptOutputs = @{}
      $DeploymentScriptOutputs['objectId'] = $app.id
      $DeploymentScriptOutputs['clientId'] = $app.appId
      $DeploymentScriptOutputs['clientSecret'] = $secret
      $DeploymentScriptOutputs['principalId'] = $principal.id
    '''
    cleanupPreference: 'OnSuccess'
    retentionInterval: 'P1D'
    forceUpdateTag: currentTime
  }
}

var cdfClientId = script.properties.outputs.clientId
var clientSecret = script.properties.outputs.clientSecret
var secretReference = '@Microsoft.KeyVault(SecretUri=https://${keyVaultName}.vault.azure.net/secrets/${appRegistrationName}-SECRET/)'

resource adt 'Microsoft.DigitalTwins/digitalTwinsInstances@2021-06-30-preview' = {
  name: adtName
  location: location
  tags: tags
  sku: {
    name: 'S1'
  }
}

resource digital_twins_endpoint_event_hub 'Microsoft.DigitalTwins/digitalTwinsInstances/endpoints@2020-12-01' = {
  name: '${adt.name}/${eventHub.name}'
  properties: {
    endpointType: 'EventHub'
    authenticationType: 'KeyBased'
    connectionStringPrimaryKey: '${listKeys(authorization_send_listen.id, authorization_send_listen.apiVersion).primaryConnectionString}'
    connectionStringSecondaryKey: '${listKeys(authorization_send_listen.id, authorization_send_listen.apiVersion).secondaryConnectionString}'
  }
}

resource script_createADTroute 'Microsoft.Resources/deploymentScripts@2019-10-01-preview' = {
  name: adtRoute
  location: location
  kind: 'AzureCLI'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    azCliVersion: '2.40.0'
    environmentVariables: [
      {
        name: 'adtName'
        value: adt.name
      }
      {
        name: 'eventHubName'
        value: eventHub.name
      }
      {
        name: 'resourceGroup'
        value: resourceGroup().name
      }      
    ]
    scriptContent: '''
      az config set extension.use_dynamic_install=yes_without_prompt
      az dt route create -n $adtName --endpoint-name $eventHubName --route-name 'eventhub' -g $resourceGroup --filter "type = 'microsoft.iot.telemetry' OR type = 'Microsoft.DigitalTwins.Twin.Create' OR type = 'Microsoft.DigitalTwins.Twin.Update' OR type = 'Microsoft.DigitalTwins.Twin.Delete' OR type = 'Microsoft.DigitalTwins.Relationship.Create' OR type = 'Microsoft.DigitalTwins.Relationship.Update' OR type = 'Microsoft.DigitalTwins.Relationship.Delete'"
    '''
    cleanupPreference: 'OnSuccess'
    retentionInterval: 'P1D'
    forceUpdateTag: currentTime
  }
}

resource keyVault 'Microsoft.KeyVault/vaults@2021-04-01-preview' = {
  name: keyVaultName
  location: location
  properties: {
    enabledForDeployment: enabledForDeployment
    enabledForTemplateDeployment: enabledForTemplateDeployment
    enabledForDiskEncryption: enabledForDiskEncryption
    tenantId: tenantId
    accessPolicies: [
      {
        objectId: functionAppCDF2ADT.identity.principalId
        tenantId: tenantId
        permissions: {
          keys: keysPermissions
          secrets: secretsPermissions
        }
      }
      {
        objectId: functionAppADT2CDF.identity.principalId
        tenantId: tenantId
        permissions: {
          keys: keysPermissions
          secrets: secretsPermissions
        }
      }
    ]
    sku: {
      name: skuName
      family: 'A'
    }
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

resource keyVaultName_secretsObject_secrets_secretName 'Microsoft.KeyVault/vaults/secrets@2021-04-01-preview' = {
  name: '${keyVaultName}/${appRegistrationName}-SECRET'
  properties: {
    value: clientSecret
  }
  dependsOn: [
    keyVault
  ]
}

resource applicationInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: applicationInsightsName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    Request_Source: 'rest'
  }
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2021-08-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: storageAccountType
  }
  kind: 'Storage'
}

resource hostingPlanADT2CDF 'Microsoft.Web/serverfarms@2021-03-01' = {
  name: hostingPlanNameADT2CDF
  location: location
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  kind: 'functionapp'
  properties: {
    reserved: true
  }
}

resource hostingPlanCDF2ADT 'Microsoft.Web/serverfarms@2021-03-01' = {
  name: hostingPlanNameCDF2ADT
  location: location
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  kind: 'functionapp'
  properties: {
    reserved: true
  }
}

resource functionAppCDF2ADT 'Microsoft.Web/sites@2021-03-01' = {
  name: functionAppNameCDF2ADT
  location: location
  kind: 'functionapp,linux'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: hostingPlanCDF2ADT.id
    reserved: true
    siteConfig: {
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccountName};EndpointSuffix=${environment().suffixes.storage};AccountKey=${listKeys(storageAccount.id, '2021-08-01').keys[0].value}'
        }
        {
          name: 'WEBSITE_CONTENTAZUREFILECONNECTIONSTRING'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccountName};EndpointSuffix=${environment().suffixes.storage};AccountKey=${listKeys(storageAccount.id, '2021-08-01').keys[0].value}'
        }
        {
          name: 'WEBSITE_CONTENTSHARE'
          value: toLower(functionAppNameCDF2ADT)
        }
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'APPINSIGHTS_INSTRUMENTATIONKEY'
          value: applicationInsights.properties.InstrumentationKey
        }
        {
          name: 'FUNCTIONS_WORKER_RUNTIME'
          value: functionWorkerRuntime
        }
        {
          name: 'ENABLE_ORYX_BUILD'
          value: 'true'
        }        
        {
          name: 'ADT_URL'
          value: 'https://${nameprefix}-ADT-${uniqueString(resourceGroup().id)}${adtDomainName}'
        }
        {
          name: 'CDF_CLIENT_SECRET'
          value: secretReference
        }
        {
          name: 'CDF_CLIENTID'
          value: cdfClientId
        }
        {
          name: 'CDF_CLUSTER'
          value: cdfCluster
        }
        {
          name: 'CDF_TENANTID'
          value: cdfTenantId
        }
        {
          name: 'CDF_PROJECT'
          value: cdfProject
        }
        {
          name: 'ROOT_ASSET_EXTERNAL_ID'
          value: rootAssetExternalID
        }
      ]
      ftpsState: 'FtpsOnly'
      minTlsVersion: '1.2'
      linuxFxVersion: 'python|3.9'
    }
    httpsOnly: true
  }
}

resource functionAppADT2CDF 'Microsoft.Web/sites@2021-03-01' = {
  name: functionAppNameADT2CDF
  location: location
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: hostingPlanADT2CDF.id
    siteConfig: {
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccountName};EndpointSuffix=${environment().suffixes.storage};AccountKey=${listKeys(storageAccount.id, '2021-08-01').keys[0].value}'
        }
        {
          name: 'WEBSITE_CONTENTAZUREFILECONNECTIONSTRING'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccountName};EndpointSuffix=${environment().suffixes.storage};AccountKey=${listKeys(storageAccount.id, '2021-08-01').keys[0].value}'
        }
        {
          name: 'WEBSITE_CONTENTSHARE'
          value: toLower(functionAppNameADT2CDF)
        }
        {
          name: 'EVENTHUB_CONNECTION_STRING'
          value: '${listKeys(authorization_send_listen.id, authorization_send_listen.apiVersion).primaryConnectionString}'
        }
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'APPINSIGHTS_INSTRUMENTATIONKEY'
          value: applicationInsights.properties.InstrumentationKey
        }
        {
          name: 'FUNCTIONS_WORKER_RUNTIME'
          value: functionWorkerRuntime
        }
        {
          name: 'ENABLE_ORYX_BUILD'
          value: 'true'
        }        
        {
          name: 'ADT_URL'
          value: 'https://${nameprefix}-ADT-${uniqueString(resourceGroup().id)}${adtDomainName}'
        }
        {
          name: 'CDF_CLIENT_SECRET'
          value: secretReference
        }
        {
          name: 'CDF_CLIENTID'
          value: cdfClientId
        }
        {
          name: 'CDF_CLUSTER'
          value: cdfCluster
        }
        {
          name: 'CDF_TENANTID'
          value: cdfTenantId
        }
        {
          name: 'CDF_PROJECT'
          value: cdfProject
        }
        {
          name: 'ROOT_ASSET_EXTERNAL_ID'
          value: rootAssetExternalID
        }
      ]
      ftpsState: 'FtpsOnly'
      minTlsVersion: '1.2'
      linuxFxVersion: 'python|3.9'
    }
    httpsOnly: true
  }
}
resource DeployCDF2ADT 'Microsoft.Web/sites/extensions@2021-03-01' = {
  name: '${functionAppCDF2ADT.name}/zipdeploy'
  properties: {
    packageUri: 'https://github.com/cognitedata/azure-digital-twin-cdf-sync/releases/download/v0.1.0/CDF2ADTSync.zip'
  }
}

resource DeployADT2CDF 'Microsoft.Web/sites/extensions@2021-03-01' = {
  name: '${functionAppADT2CDF.name}/zipdeploy'
  properties: {
    packageUri: 'https://github.com/cognitedata/azure-digital-twin-cdf-sync/releases/download/v0.1.0/ADT2CDFSync.zip'
  }
}

/*
resource funcCDF2ADT 'Microsoft.Web/sites/functions@2021-03-01' = {
  name: 'CDF2ADT'
  parent: functionAppCDF2ADT
  properties: {
    config: {
      disabled: false
      bindings: [
        {
          type: 'eventHubTrigger'
          name: eventHubName
          direction: 'in'
          eventHubName: eventHubNamespace.name
          connection: 'EVENTHUB_ACCESS_KEY'
          cardinality: 'many'
          consumerGroup: '$Default'
          dataType: 'binary'
        }
      ]
    }
    files: {
      'ADT2CDFSync/__init__.py': loadTextContent('../Functions/ADT2CDF/ADT2CDFSync/__init__.py')
      'ADT2CDFSync/handler.py': loadTextContent('../Functions/ADT2CDF/ADT2CDFSync/handler.py')
      'requirements.txt': loadTextContent('../Functions/ADT2CDF/requirements.txt')
    }
  }
}
*/
