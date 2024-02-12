## Selective Azure Data Factory CI/CD plug-n-play DevOps templates

# Summary
Templates for selective deployment of Azure Data Factory using [azure.datafactory.tools](https://github.com/Azure-Player/azure.datafactory.tools) with Azure DevOps.

# In Azure DevOps

1. In Azure Data Factory
    1. Setup ADF sync repo in your data factory (DEV instance only)
       - Recommended to use Root Folder path **/data-factory/** or **/data-factory/<data_factory_name>** if you have many ADFs.
2. In Azure DevOps
    1. Create folder **/devops/generic** and then, in that folder create **adf-selective-deploy-job-template.yml** file:
        
        ```YAML
        parameters:
        - name: environmentName 
          type: string 

        - name: rootFolder 
          type: string 

        - name: dataFactoryName 
          type: string 

        - name: location 
          type: string

        - name: resourceGroupName 
          type: string

        - name: configPath 
          type: string

        - name: serviceConnectionName 
          type: string 

        - name: deploymentScriptPath 
          type: string 

        jobs:
        - deployment: ${{ parameters.environmentName }}
          displayName: Deployment to ${{ parameters.environmentName }}
          environment: '${{ parameters.environmentName }}'
          strategy:
            runOnce:
              deploy:
                steps:
                - checkout: self 

                - powershell: |
                    Install-Module Az.DataFactory -MinimumVersion "1.10.0" -Force
                    Install-Module -Name "azure.datafactory.tools" -Force
                    Import-Module -Name "azure.datafactory.tools" -Force
                    Get-Module -Name azure.datafactory.tools
                    Get-Module -Name Az.DataFactory
                  displayName: 'Install and import PowerShell modules'

                - powershell: |
                    Test-AdfCode -RootFolder "${{ parameters.rootFolder }}"
                  displayName: 'Test Azure Data Factory code'
                
                - task: AzurePowerShell@5
                  displayName: 'Deploy Azure Data Factory'
                  inputs:
                    azureSubscription: '${{ parameters.serviceConnectionName }}'
                    ScriptPath: '${{ parameters.deploymentScriptPath }}'
                    ScriptArguments: "-rootFolder ${{ parameters.rootFolder }} \
                      -dataFactoryName ${{ parameters.dataFactoryName }} \
                      -resourceGroupName ${{ parameters.resourceGroupName }} \
                      -location '${{ parameters.location }}' \
                      -configPath ${{ parameters.configPath }}"        
                    FailOnStandardError: true
                    azurePowerShellVersion: LatestVersion
        ```
    1. Create folder **/devops/config** and then, in that folder create **DEV.csv** configuration file and fill it with configuration for your **DEV** environment:
        ```CSV
        type,name,path,value
        # Shared section - common linked services, global parameters, common properties, etc.
        linkedService,ADLSg2_LS,typeProperties.url,"https://tybuldatalakedemo.dfs.core.windows.net/"

        # Team1 section
        pipeline,Team_1_Pipeline,$.properties.activities[0].description,"Description for DEV environment"
        ```
    1. Create similar configuration files for your remaining environments, e.g. **UAT** and **PROD**.
    1. Navigate to **Project Settings >> Service Connections** and create new connection to Azure using Service Principal and grant at least **Data Factory Contributor** role to all data factories that you will be deploying to:

          1. In Azure Portal navigate to Azure Active Directory and create new App Registration
          1. For ADF only piplines grant **Data Factory Contibutor** role on Azure Data Factory resource, or for full CI/CD in Azure grant **Contributor** role to an entire resource group
          1. Copy the details of this service principal and subscription to Azure DevOps
    1. Create Environment (**Pipelines >> Environment**) for every environment to which you want to deploy your ADF.
    1. Optionally, create Variable Group (**Pipelines >> Library >> Variable Groups**) for every environment. Do it only if you want to reference variables in your configuration files.
    1. Create folder **/devops/scope/everything** and then, in that folder create **deploy-script.ps1** file:
        ```PS
        param
        (
            [parameter(Mandatory = $true)] [String] $rootFolder,
            [parameter(Mandatory = $true)] [String] $dataFactoryName,
            [parameter(Mandatory = $true)] [String] $resourceGroupName,
            [parameter(Mandatory = $true)] [String] $location,
            [parameter(Mandatory = $true)] [String] $configPath
        )
        $opt = New-AdfPublishOption

        # Don't provision new instance of ADF if it doesn't exist
        $opt.CreateNewInstance = $false
        # Delete objects that are not in the source code
        $opt.DeleteNotInSource = $true
        Publish-AdfV2FromJson -RootFolder "$rootFolder" -ResourceGroupName "$resourceGroupName" -DataFactoryName "$dataFactoryName" -Location "$location" -Option $opt -Stage "$configPath"
        ```
    1. Navigate to **/devops/scope/everything** folder and create **pipeline.yml** file (replace temp variables to match your **DEV** environment as this pipeline is used to sync **main** branch with **DEV** environment):
        ```YAML
        trigger:
        - main

        pool:
          vmImage: ubuntu-latest

        stages:
        - stage: '<stage_name>'
          variables:
          - group: '<variable_group_name (if_used)>'
          jobs:
          - template: '../../generic/adf-selective-deploy-job-template.yml'
            parameters:
              environmentName: '<environment_name>'
              rootFolder: '$(System.DefaultWorkingDirectory)/data-factory'
              dataFactoryName: '<data_factory_name>'
              location: '<data_factory_location>'
              resourceGroupName: '<resource_group_name>'
              configPath: '$(System.DefaultWorkingDirectory)/devops/config/<config_file_name>.csv'
              serviceConnectionName: '<service_connection_name>'
              deploymentScriptPath: '$(System.DefaultWorkingDirectory)/devops/scope/everything/deploy-script.ps1'
        
        ```
    1. Create new pipeline and use **pipeline.yml** file as its source. Rename the pipeline if necessary.
    1. For every team/project that requires a selective deployment:
        1. Create a dedicated directory under **/devops/scope/**, e.g. **/devops/scope/Product1**.
        1. In that folder, create **deploy-script.ps1** file and customize objects to be deployed:
            ```PS
            param
            (
                [parameter(Mandatory = $true)] [String] $rootFolder,
                [parameter(Mandatory = $true)] [String] $dataFactoryName,
                [parameter(Mandatory = $true)] [String] $resourceGroupName,
                [parameter(Mandatory = $true)] [String] $location,
                [parameter(Mandatory = $true)] [String] $configPath
            )
            $adf = Import-AdfFromFolder -RootFolder "$rootFolder" -FactoryName $dataFactoryName
            $opt = New-AdfPublishOption

            ##################### THIS IS THE PART THAT REQUIRES CUSTOMIZATION ####################################
            # Team1 artifacts to be deployed by this script
            # Pipelines and datasets from Team_1 folder in ADF:
            $selectedObjects = $adf.GetObjectsByFolderName('Team_1')
            # Linked services:
            $opt.Includes.Add("linkedService.ADLSg2_LS", "")
            # Integration runtime:
            $opt.Includes.Add("integrationruntime.*", "")
            # Data Factory object (includes global params):
            $opt.Includes.Add("factory.*", "")
            ################################ END OF CUSTOMIZED PART ###############################################

            $opt.Includes += $selectedObjects
            # Don't provision new instance of ADF if it doesn't exist
            $opt.CreateNewInstance = $false
            $opt.TriggerStopMethod = "DeployableOnly"
            Publish-AdfV2FromJson -RootFolder "$rootFolder" -ResourceGroupName "$resourceGroupName" -DataFactoryName "$dataFactoryName" -Location "$location" -Option $opt -Stage "$configPath"
            ```
        1. In that folder, create **pipeline.yml** file with as many stages as you need - one for every targe environment (replace temp variables):
              ```YAML
              trigger: none # Disable CI triggers as this pipeline is to be started manually
            
              pool:
                vmImage: ubuntu-latest

              stages:
              - stage: '<stage_name>'
                variables:
                - group: '<variable_group_name (if used)>'
                jobs:
                - template: '../../generic/adf-selective-deploy-job-template.yml'
                  parameters:
                    environmentName: '<environment_name>'
                    rootFolder: '$(System.DefaultWorkingDirectory)/data-factory'
                    dataFactoryName: '<data_factory_name>'
                    location: '<data_factory_location>'
                    resourceGroupName: '<resource_group_name>'
                    configPath: '$(System.DefaultWorkingDirectory)/devops/config/<config_file_name>.csv'
                    serviceConnectionName: '<service_connection_name>'
                    deploymentScriptPath: '$(System.DefaultWorkingDirectory)/devops/scope/<team_or_project>/deploy-script.ps1'
              
              - stage: '<stage_name>'
                variables:
                - group: '<variable_group_name (if used)>'
                jobs:
                - template: '../../generic/adf-selective-deploy-job-template.yml'
                  parameters:
                    environmentName: '<environment_name>'
                    rootFolder: '$(System.DefaultWorkingDirectory)/data-factory'
                    dataFactoryName: '<data_factory_name>'
                    location: '<data_factory_location>'
                    resourceGroupName: '<resource_group_name>'
                    configPath: '$(System.DefaultWorkingDirectory)/devops/config/<config_file_name>.csv'
                    serviceConnectionName: '<service_connection_name>'
                    deploymentScriptPath: '$(System.DefaultWorkingDirectory)/devops/scope/<team_or_project>/deploy-script.ps1'
              ```
        1. Create new pipeline and use **pipeline.yml** file as its source. Rename the pipeline if necessary.

