# Databricks notebook source
# MAGIC %md
# MAGIC ## カタログ名とスキーマ名を定義

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
catalog_name = dbutils.widgets.get("catalog_name")
print(f"catalog_name is {catalog_name}")

dbutils.widgets.text("schema_name", "")
schema_name = dbutils.widgets.get("schema_name")
print(f"schema_name is {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 共通処理

# COMMAND ----------

import inspect


def create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=False,
):
    if should_replace:
        create_state = "CREATE OR REPLACE"
    else:
        create_state = "CREATE"
    schema = inspect.cleandoc(schema)
    create_ddl = """
    {create_state} TABLE {catalog_name}.{schema_name}.{table_name}
    (
    {schema}
    )
    """
    create_ddl = inspect.cleandoc(create_ddl)
    create_ddl = create_ddl.format(
        create_state=create_state,
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
        schema=schema,
    )
    print(create_ddl)
    return spark.sql(create_ddl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Account (取引先)

# COMMAND ----------

table_name = "account"
schema = """
`Id` STRING NOT NULL, 
`IsDeleted` BOOLEAN, 
`MasterRecordId` STRING, 
`Name` STRING, 
`Type` STRING, 
`ParentId` STRING, 
`BillingStreet` STRING, 
`BillingCity` STRING, 
`BillingState` STRING, 
`BillingPostalCode` STRING, 
`BillingCountry` STRING, 
`BillingLatitude` DECIMAL(18, 15), 
`BillingLongitude` DECIMAL(18, 15), 
`BillingGeocodeAccuracy` STRING, 
`ShippingStreet` STRING, 
`ShippingCity` STRING, 
`ShippingState` STRING, 
`ShippingPostalCode` STRING, 
`ShippingCountry` STRING, 
`ShippingLatitude` DECIMAL(18, 15), 
`ShippingLongitude` DECIMAL(18, 15), 
`ShippingGeocodeAccuracy` STRING, 
`Phone` STRING, 
`Fax` STRING, 
`AccountNumber` STRING, 
`Website` STRING, 
`PhotoUrl` STRING, 
`Sic` STRING, 
`Industry` STRING, 
`AnnualRevenue` DECIMAL(18, 0), 
`NumberOfEmployees` INT, 
`Ownership` STRING, 
`TickerSymbol` STRING, 
`Description` STRING, 
`Rating` STRING, 
`Site` STRING, 
`OwnerId` STRING, 
`CreatedDate` TIMESTAMP, 
`CreatedById` STRING, 
`LastModifiedDate` TIMESTAMP, 
`LastModifiedById` STRING, 
`SystemModstamp` TIMESTAMP, 
`LastActivityDate` DATE, 
`LastViewedDate` TIMESTAMP, 
`LastReferencedDate` TIMESTAMP, 
`Jigsaw` STRING, 
`JigsawCompanyId` STRING, 
`CleanStatus` STRING, 
`AccountSource` STRING, 
`DunsNumber` STRING, 
`Tradestyle` STRING, 
`NaicsCode` STRING, 
`NaicsDesc` STRING, 
`YearStarted` STRING, 
`SicDesc` STRING, 
`DandbCompanyId` STRING
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract (契約)

# COMMAND ----------

table_name = "contact"
schema = """
`Id` STRING NOT NULL,
`IsDeleted` BOOLEAN,
`MasterRecordId` STRING,
`AccountId` STRING,
`LastName` STRING,
`FirstName` STRING,
`Salutation` STRING,
`Name` STRING,
`OtherStreet` STRING,
`OtherCity` STRING,
`OtherState` STRING,
`OtherPostalCode` STRING,
`OtherCountry` STRING,
`OtherLatitude` DECIMAL(18, 15),
`OtherLongitude` DECIMAL(18, 15),
`OtherGeocodeAccuracy` STRING,
`MailingStreet` STRING,
`MailingCity` STRING,
`MailingState` STRING,
`MailingPostalCode` STRING,
`MailingCountry` STRING,
`MailingLatitude` DECIMAL(18, 15),
`MailingLongitude` DECIMAL(18, 15),
`MailingGeocodeAccuracy` STRING,
`Phone` STRING,
`Fax` STRING,
`MobilePhone` STRING,
`HomePhone` STRING,
`OtherPhone` STRING,
`AssistantPhone` STRING,
`ReportsToId` STRING,
`Email` STRING,
`Title` STRING,
`Department` STRING,
`AssistantName` STRING,
`LeadSource` STRING,
`Birthdate` DATE,
`Description` STRING,
`OwnerId` STRING,
`CreatedDate` TIMESTAMP,
`CreatedById` STRING,
`LastModifiedDate` TIMESTAMP,
`LastModifiedById` STRING,
`SystemModstamp` TIMESTAMP,
`LastActivityDate` DATE,
`LastCURequestDate` TIMESTAMP,
`LastCUUpdateDate` TIMESTAMP,
`LastViewedDate` TIMESTAMP,
`LastReferencedDate` TIMESTAMP,
`EmailBouncedReason` STRING,
`EmailBouncedDate` TIMESTAMP,
`IsEmailBounced` BOOLEAN,
`PhotoUrl` STRING,
`Jigsaw` STRING,
`JigsawContactId` STRING,
`CleanStatus` STRING,
`IndividualId` STRING,
`IsPriorityRecord` BOOLEAN,
`ContactSource` STRING
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lead (リード)

# COMMAND ----------

table_name = "lead"
schema = """
`Id` STRING NOT NULL,
`IsDeleted` BOOLEAN,
`MasterRecordId` STRING,
`LastName` STRING,
`FirstName` STRING,
`Salutation` STRING,
`Name` STRING,
`Title` STRING,
`Company` STRING,
`Street` STRING,
`City` STRING,
`State` STRING,
`PostalCode` STRING,
`Country` STRING,
`Latitude` DECIMAL(18, 15),
`Longitude` DECIMAL(18, 15),
`GeocodeAccuracy` STRING,
`Phone` STRING,
`MobilePhone` STRING,
`Fax` STRING,
`Email` STRING,
`Website` STRING,
`PhotoUrl` STRING,
`Description` STRING,
`LeadSource` STRING,
`Status` STRING,
`Industry` STRING,
`Rating` STRING,
`AnnualRevenue` DECIMAL(18, 0),
`NumberOfEmployees` INT,
`OwnerId` STRING,
`IsConverted` BOOLEAN,
`ConvertedDate` DATE,
`ConvertedAccountId` STRING,
`ConvertedContactId` STRING,
`ConvertedOpportunityId` STRING,
`IsUnreadByOwner` BOOLEAN,
`CreatedDate` TIMESTAMP,
`CreatedById` STRING,
`LastModifiedDate` TIMESTAMP,
`LastModifiedById` STRING,
`SystemModstamp` TIMESTAMP,
`LastActivityDate` DATE,
`LastViewedDate` TIMESTAMP,
`LastReferencedDate` TIMESTAMP,
`Jigsaw` STRING,
`JigsawContactId` STRING,
`CleanStatus` STRING,
`CompanyDunsNumber` STRING,
`DandbCompanyId` STRING,
`EmailBouncedReason` STRING,
`EmailBouncedDate` TIMESTAMP,
`IndividualId` STRING,
`IsPriorityRecord` BOOLEAN
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Campaign (キャンペーン)

# COMMAND ----------

table_name = "campaign"
schema = """
`Id` STRING NOT NULL,
`IsDeleted` BOOLEAN,
`Name` STRING,
`ParentId` STRING,
`Type` STRING,
`Status` STRING,
`StartDate` DATE,
`EndDate` DATE,
`ExpectedRevenue` DECIMAL(18, 0),
`BudgetedCost` DECIMAL(18, 0),
`ActualCost` DECIMAL(18, 0),
`ExpectedResponse` DECIMAL(10, 2),
`NumberSent` DECIMAL(18, 0),
`IsActive` BOOLEAN,
`Description` STRING,
`NumberOfLeads` INT,
`NumberOfConvertedLeads` INT,
`NumberOfContacts` INT,
`NumberOfResponses` INT,
`NumberOfOpportunities` INT,
`NumberOfWonOpportunities` INT,
`AmountAllOpportunities` DECIMAL(18, 0),
`AmountWonOpportunities` DECIMAL(18, 0),
`OwnerId` STRING,
`CreatedDate` TIMESTAMP,
`CreatedById` STRING,
`LastModifiedDate` TIMESTAMP,
`LastModifiedById` STRING,
`SystemModstamp` TIMESTAMP,
`LastActivityDate` DATE,
`LastViewedDate` TIMESTAMP,
`LastReferencedDate` TIMESTAMP,
`CampaignMemberRecordTypeId` STRING
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opportunity (商談)

# COMMAND ----------

table_name = "opportunity"
schema = """
`Id` STRING NOT NULL,
`IsDeleted` BOOLEAN,
`AccountId` STRING,
`IsPrivate` BOOLEAN,
`Name` STRING,
`Description` STRING,
`RecordTypeId` STRING,
`StageName` STRING,
`Amount` DECIMAL(16, 0),
`Probability` DECIMAL(3, 0),
`ExpectedRevenue` DECIMAL(16, 0),
`TotalOpportunityQuantity` DECIMAL(18, 2),
`CloseDate` DATE,
`Type` STRING,
`NextStep` STRING,
`LeadSource` STRING,
`IsClosed` BOOLEAN,
`IsWon` BOOLEAN,
`ForecastCategory` STRING,
`ForecastCategoryName` STRING,
`CampaignId` STRING,
`HasOpportunityLineItem` BOOLEAN,
`Pricebook2Id` STRING,
`OwnerId` STRING,
`CreatedDate` TIMESTAMP,
`CreatedById` STRING,
`LastModifiedDate` TIMESTAMP,
`LastModifiedById` STRING,
`SystemModstamp` TIMESTAMP,
`LastActivityDate` DATE,
`PushCount` INT,
`LastStageChangeDate` TIMESTAMP,
`FiscalQuarter` INT,
`FiscalYear` INT,
`Fiscal` STRING,
`ContactId` STRING,
`LastViewedDate` TIMESTAMP,
`LastReferencedDate` TIMESTAMP,
`HasOpenActivity` BOOLEAN,
`HasOverdueTask` BOOLEAN,
`LastAmountChangedHistoryId` STRING,
`LastCloseDateChangedHistoryId` STRING,
`CurrencyIsoCode` STRING,
`Product2Id__c` STRING
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product (製品)

# COMMAND ----------

table_name = "product2"
schema = """
`Id` STRING NOT NULL,
`Name` STRING,
`ProductCode` STRING,
`Description` STRING,
`IsActive` BOOLEAN,
`CreatedDate` TIMESTAMP,
`CreatedById` STRING,
`LastModifiedDate` TIMESTAMP,
`LastModifiedById` STRING,
`SystemModstamp` TIMESTAMP,
`Family` STRING,
`ExternalDataSourceId` STRING,
`ExternalId` STRING,
`DisplayUrl` STRING,
`QuantityUnitOfMeasure` STRING,
`IsDeleted` BOOLEAN,
`IsArchived` BOOLEAN,
`LastViewedDate` TIMESTAMP,
`LastReferencedDate` TIMESTAMP,
`StockKeepingUnit` STRING
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Book Entry (価格表エントリ)

# COMMAND ----------

table_name = "pricebook_entry"
schema = """
`Id` STRING NOT NULL,
`Name` STRING,
`Pricebook2Id` STRING,
`Product2Id` STRING,
`UnitPrice` DECIMAL(16, 0),
`IsActive` BOOLEAN,
`UseStandardPrice` BOOLEAN,
`CreatedDate` TIMESTAMP,
`CreatedById` STRING,
`LastModifiedDate` TIMESTAMP,
`LastModifiedById` STRING,
`SystemModstamp` TIMESTAMP,
`ProductCode` STRING,
`IsDeleted` BOOLEAN,
`IsArchived` BOOLEAN
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Case (ケース)

# COMMAND ----------

table_name = "case"
schema = """
`Id` STRING NOT NULL,
`IsDeleted` BOOLEAN,
`MasterRecordId` STRING,
`CaseNumber` STRING,
`ContactId` STRING,
`AccountId` STRING,
`AssetId` STRING,
`ParentId` STRING,
`SuppliedName` STRING,
`SuppliedEmail` STRING,
`SuppliedPhone` STRING,
`SuppliedCompany` STRING,
`Type` STRING,
`Status` STRING,
`Reason` STRING,
`Origin` STRING,
`Subject` STRING,
`Priority` STRING,
`Description` STRING,
`IsClosed` BOOLEAN,
`ClosedDate` TIMESTAMP,
`IsEscalated` BOOLEAN,
`OwnerId` STRING,
`CreatedDate` TIMESTAMP,
`CreatedById` STRING,
`LastModifiedDate` TIMESTAMP,
`LastModifiedById` STRING,
`SystemModstamp` TIMESTAMP,
`ContactPhone` STRING,
`ContactMobile` STRING,
`ContactEmail` STRING,
`ContactFax` STRING,
`Comments` STRING,
`LastViewedDate` TIMESTAMP,
`LastReferencedDate` TIMESTAMP,
`Product2Id__c` STRING
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## User（ユーザ）

# COMMAND ----------

table_name = "user"
schema = """
`Id` STRING NOT NULL,
`Username` STRING,
`LastName` STRING,
`FirstName` STRING,
`Name` STRING,
`CompanyName` STRING,
`Division` STRING,
`Department` STRING,
`Title` STRING,
`Street` STRING,
`City` STRING,
`State` STRING,
`PostalCode` STRING,
`Country` STRING,
`Latitude` DECIMAL(18, 15),
`Longitude` DECIMAL(18, 15),
`GeocodeAccuracy` STRING,
`Email` STRING,
`EmailPreferencesAutoBcc` BOOLEAN,
`EmailPreferencesAutoBccStayInTouch` BOOLEAN,
`EmailPreferencesStayInTouchReminder` BOOLEAN,
`SenderEmail` STRING,
`SenderName` STRING,
`Signature` STRING,
`StayInTouchSubject` STRING,
`StayInTouchSignature` STRING,
`StayInTouchNote` STRING,
`Phone` STRING,
`Fax` STRING,
`MobilePhone` STRING,
`Alias` STRING,
`CommunityNickname` STRING,
`BadgeText` STRING,
`IsActive` BOOLEAN,
`TimeZoneSidKey` STRING,
`UserRoleId` STRING,
`LocaleSidKey` STRING,
`ReceivesInfoEmails` BOOLEAN,
`ReceivesAdminInfoEmails` BOOLEAN,
`EmailEncodingKey` STRING,
`ProfileId` STRING,
`UserType` STRING,
`LanguageLocaleKey` STRING,
`EmployeeNumber` STRING,
`DelegatedApproverId` STRING,
`ManagerId` STRING,
`LastLoginDate` TIMESTAMP,
`LastPasswordChangeDate` TIMESTAMP,
`CreatedDate` TIMESTAMP,
`CreatedById` STRING,
`LastModifiedDate` TIMESTAMP,
`LastModifiedById` STRING,
`SystemModstamp` TIMESTAMP,
`NumberOfFailedLogins` INT,
`OfflineTrialExpirationDate` TIMESTAMP,
`OfflinePdaTrialExpirationDate` TIMESTAMP,
`UserPermissionsMarketingUser` BOOLEAN,
`UserPermissionsOfflineUser` BOOLEAN,
`UserPermissionsCallCenterAutoLogin` BOOLEAN,
`UserPermissionsSFContentUser` BOOLEAN,
`UserPermissionsKnowledgeUser` BOOLEAN,
`UserPermissionsInteractionUser` BOOLEAN,
`UserPermissionsSupportUser` BOOLEAN,
`UserPermissionsJigsawProspectingUser` BOOLEAN,
`UserPermissionsSiteforceContributorUser` BOOLEAN,
`UserPermissionsSiteforcePublisherUser` BOOLEAN,
`UserPermissionsWorkDotComUserFeature` BOOLEAN,
`ForecastEnabled` BOOLEAN,
`UserPreferencesActivityRemindersPopup` BOOLEAN,
`UserPreferencesEventRemindersCheckboxDefault` BOOLEAN,
`UserPreferencesTaskRemindersCheckboxDefault` BOOLEAN,
`UserPreferencesReminderSoundOff` BOOLEAN,
`UserPreferencesDisableAllFeedsEmail` BOOLEAN,
`UserPreferencesDisableFollowersEmail` BOOLEAN,
`UserPreferencesDisableProfilePostEmail` BOOLEAN,
`UserPreferencesDisableChangeCommentEmail` BOOLEAN,
`UserPreferencesDisableLaterCommentEmail` BOOLEAN,
`UserPreferencesDisProfPostCommentEmail` BOOLEAN,
`UserPreferencesContentNoEmail` BOOLEAN,
`UserPreferencesContentEmailAsAndWhen` BOOLEAN,
`UserPreferencesApexPagesDeveloperMode` BOOLEAN,
`UserPreferencesReceiveNoNotificationsAsApprover` BOOLEAN,
`UserPreferencesReceiveNotificationsAsDelegatedApprover` BOOLEAN,
`UserPreferencesHideCSNGetChatterMobileTask` BOOLEAN,
`UserPreferencesDisableMentionsPostEmail` BOOLEAN,
`UserPreferencesDisMentionsCommentEmail` BOOLEAN,
`UserPreferencesHideCSNDesktopTask` BOOLEAN,
`UserPreferencesHideChatterOnboardingSplash` BOOLEAN,
`UserPreferencesHideSecondChatterOnboardingSplash` BOOLEAN,
`UserPreferencesDisCommentAfterLikeEmail` BOOLEAN,
`UserPreferencesDisableLikeEmail` BOOLEAN,
`UserPreferencesSortFeedByComment` BOOLEAN,
`UserPreferencesDisableMessageEmail` BOOLEAN,
`UserPreferencesJigsawListUser` BOOLEAN,
`UserPreferencesDisableBookmarkEmail` BOOLEAN,
`UserPreferencesDisableSharePostEmail` BOOLEAN,
`UserPreferencesEnableAutoSubForFeeds` BOOLEAN,
`UserPreferencesDisableFileShareNotificationsForApi` BOOLEAN,
`UserPreferencesShowTitleToExternalUsers` BOOLEAN,
`UserPreferencesShowManagerToExternalUsers` BOOLEAN,
`UserPreferencesShowEmailToExternalUsers` BOOLEAN,
`UserPreferencesShowWorkPhoneToExternalUsers` BOOLEAN,
`UserPreferencesShowMobilePhoneToExternalUsers` BOOLEAN,
`UserPreferencesShowFaxToExternalUsers` BOOLEAN,
`UserPreferencesShowStreetAddressToExternalUsers` BOOLEAN,
`UserPreferencesShowCityToExternalUsers` BOOLEAN,
`UserPreferencesShowStateToExternalUsers` BOOLEAN,
`UserPreferencesShowPostalCodeToExternalUsers` BOOLEAN,
`UserPreferencesShowCountryToExternalUsers` BOOLEAN,
`UserPreferencesShowProfilePicToGuestUsers` BOOLEAN,
`UserPreferencesShowTitleToGuestUsers` BOOLEAN,
`UserPreferencesShowCityToGuestUsers` BOOLEAN,
`UserPreferencesShowStateToGuestUsers` BOOLEAN,
`UserPreferencesShowPostalCodeToGuestUsers` BOOLEAN,
`UserPreferencesShowCountryToGuestUsers` BOOLEAN,
`UserPreferencesShowForecastingChangeSignals` BOOLEAN,
`UserPreferencesLiveAgentMiawSetupDeflection` BOOLEAN,
`UserPreferencesHideS1BrowserUI` BOOLEAN,
`UserPreferencesDisableEndorsementEmail` BOOLEAN,
`UserPreferencesPathAssistantCollapsed` BOOLEAN,
`UserPreferencesCacheDiagnostics` BOOLEAN,
`UserPreferencesShowEmailToGuestUsers` BOOLEAN,
`UserPreferencesShowManagerToGuestUsers` BOOLEAN,
`UserPreferencesShowWorkPhoneToGuestUsers` BOOLEAN,
`UserPreferencesShowMobilePhoneToGuestUsers` BOOLEAN,
`UserPreferencesShowFaxToGuestUsers` BOOLEAN,
`UserPreferencesShowStreetAddressToGuestUsers` BOOLEAN,
`UserPreferencesLightningExperiencePreferred` BOOLEAN,
`UserPreferencesPreviewLightning` BOOLEAN,
`UserPreferencesHideEndUserOnboardingAssistantModal` BOOLEAN,
`UserPreferencesHideLightningMigrationModal` BOOLEAN,
`UserPreferencesHideSfxWelcomeMat` BOOLEAN,
`UserPreferencesHideBiggerPhotoCallout` BOOLEAN,
`UserPreferencesGlobalNavBarWTShown` BOOLEAN,
`UserPreferencesGlobalNavGridMenuWTShown` BOOLEAN,
`UserPreferencesCreateLEXAppsWTShown` BOOLEAN,
`UserPreferencesFavoritesWTShown` BOOLEAN,
`UserPreferencesRecordHomeSectionCollapseWTShown` BOOLEAN,
`UserPreferencesRecordHomeReservedWTShown` BOOLEAN,
`UserPreferencesFavoritesShowTopFavorites` BOOLEAN,
`UserPreferencesExcludeMailAppAttachments` BOOLEAN,
`UserPreferencesSuppressTaskSFXReminders` BOOLEAN,
`UserPreferencesSuppressEventSFXReminders` BOOLEAN,
`UserPreferencesPreviewCustomTheme` BOOLEAN,
`UserPreferencesHasCelebrationBadge` BOOLEAN,
`UserPreferencesUserDebugModePref` BOOLEAN,
`UserPreferencesSRHOverrideActivities` BOOLEAN,
`UserPreferencesNewLightningReportRunPageEnabled` BOOLEAN,
`UserPreferencesReverseOpenActivitiesView` BOOLEAN,
`UserPreferencesHasSentWarningEmail` BOOLEAN,
`UserPreferencesHasSentWarningEmail238` BOOLEAN,
`UserPreferencesHasSentWarningEmail240` BOOLEAN,
`UserPreferencesNativeEmailClient` BOOLEAN,
`UserPreferencesShowForecastingRoundedAmounts` BOOLEAN,
`ContactId` STRING,
`AccountId` STRING,
`CallCenterId` STRING,
`Extension` STRING,
`FederationIdentifier` STRING,
`AboutMe` STRING,
`FullPhotoUrl` STRING,
`SmallPhotoUrl` STRING,
`IsExtIndicatorVisible` BOOLEAN,
`OutOfOfficeMessage` STRING,
`MediumPhotoUrl` STRING,
`DigestFrequency` STRING,
`DefaultGroupNotificationFrequency` STRING,
`JigsawImportLimitOverride` INT,
`LastViewedDate` TIMESTAMP,
`LastReferencedDate` TIMESTAMP,
`BannerPhotoUrl` STRING,
`SmallBannerPhotoUrl` STRING,
`MediumBannerPhotoUrl` STRING,
`IsProfilePhotoActive` BOOLEAN,
`IndividualId` STRING,
`ProductId__c` STRING
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order（受注）

# COMMAND ----------

table_name = "order"
schema = """
`Id` STRING NOT NULL,
`OwnerId` STRING,
`ContractId` STRING,
`CurrencyIsoCode` STRING,
`AccountId` STRING,
`Pricebook2Id` STRING,
`OriginalOrderId` STRING,
`EffectiveDate` DATE,
`EndDate` DATE,
`IsReductionOrder` BOOLEAN,
`Status` STRING,
`Description` STRING,
`CustomerAuthorizedById` STRING,
`CustomerAuthorizedDate` DATE,
`CompanyAuthorizedById` STRING,
`CompanyAuthorizedDate` DATE,
`Type` STRING,
`BillingStreet` STRING,
`BillingCity` STRING,
`BillingState` STRING,
`BillingPostalCode` STRING,
`BillingCountry` STRING,
`BillingLatitude` DECIMAL(18, 15),
`BillingLongitude` DECIMAL(18, 15),
`BillingGeocodeAccuracy` STRING,
`ShippingStreet` STRING,
`ShippingCity` STRING,
`ShippingState` STRING,
`ShippingPostalCode` STRING,
`ShippingCountry` STRING,
`ShippingLatitude` DECIMAL(18, 15),
`ShippingLongitude` DECIMAL(18, 15),
`ShippingGeocodeAccuracy` STRING,
`Name` STRING,
`PoDate` DATE,
`PoNumber` STRING,
`OrderReferenceNumber` STRING,
`BillToContactId` STRING,
`ShipToContactId` STRING,
`ActivatedDate` TIMESTAMP,
`ActivatedById` STRING,
`StatusCode` STRING,
`OrderNumber` STRING,
`TotalAmount` DECIMAL(16, 0),
`CreatedDate` TIMESTAMP,
`CreatedById` STRING,
`LastModifiedDate` TIMESTAMP,
`LastModifiedById` STRING,
`IsDeleted` BOOLEAN,
`SystemModstamp` TIMESTAMP,
`LastViewedDate` TIMESTAMP,
`LastReferencedDate` TIMESTAMP,
`OpportunityId__c` STRING
"""

# COMMAND ----------

create_table(
    catalog_name,
    schema_name,
    table_name,
    schema,
    should_replace=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## end
