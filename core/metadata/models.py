"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

This file is part of elevata.

elevata is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

elevata is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with elevata. If not, see <https://www.gnu.org/licenses/>.

Contact: <https://github.com/elevata-labs/elevata>.
"""

import os
import hashlib
from django.conf import settings
from django.db import models
from django.utils import timezone
from django.core.validators import RegexValidator
from crum import get_current_user
from generic import display_key
from metadata.constants import (
  TYPE_CHOICES, INGEST_CHOICES, INCREMENT_INTERVAL_CHOICES, DATATYPE_CHOICES, 
  MATERIALIZATION_CHOICES, RELATIONSHIP_TYPE_CHOICES, PII_LEVEL_CHOICES, TARGET_DATASET_INPUT_ROLE_CHOICES,
  ACCESS_INTENT_CHOICES, ROLE_CHOICES, SENSITIVITY_CHOICES, ENVIRONMENT_CHOICES, LINEAGE_ORIGIN_CHOICES)

SHORT_NAME_VALIDATOR = RegexValidator(regex=r"^[a-z][a-z0-9]{0,9}$", 
  message=(
    "Must start with a lowercase letter and contain only lowercase letters and digits. "
    "Max length 10."
  )
)
TARGET_IDENTIFIER_VALIDATOR = RegexValidator(regex=r'^[a-z][a-z0-9_]{0,62}$',
  message=(
    "Must start with a lowercase letter and contain only lowercase letters, "
    "digits, and underscores. Max length 63."
  )
)

class AuditFields(models.Model):
  created_at = models.DateTimeField(auto_now_add=True, db_index=True)
  updated_at = models.DateTimeField(auto_now=True, db_index=True)
  created_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, editable=False,
    on_delete=models.SET_NULL, related_name="+")
  updated_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, editable=False,
    on_delete=models.SET_NULL, related_name="+")

  class Meta:
      abstract = True

  def save(self, *args, **kwargs):
    user = get_current_user()
    if user and not user.is_anonymous:
      # created_by only set once during creation
      if not self.pk and not self.created_by:
        self.created_by = user
      self.updated_by = user
    super().save(*args, **kwargs)

# -------------------------------------------------------------------
# PartialLoad
# -------------------------------------------------------------------
class PartialLoad(AuditFields):
  name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR], unique=True,
    help_text="Short code for this partial load. Will be displayed as the load pipeline name."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional description which purpose this partial load is meant for."
  )

  class Meta:
    db_table = "partial_load"
    ordering = ["name"]
    verbose_name_plural = "Partial Loads"

  def __str__(self):
    return self.name

# -------------------------------------------------------------------
# Team
# -------------------------------------------------------------------
class Team(AuditFields):
  name = models.CharField(max_length=30, unique=True,
    help_text="Name of the team, eg. Sales, Finance, ..."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional description of the team."
  )

  class Meta:
    db_table = "team"
    ordering = ["name"]
    verbose_name_plural = "Teams"

  def __str__(self):
    return self.name

# -------------------------------------------------------------------
# Person
# -------------------------------------------------------------------
class Person(AuditFields):
  email = models.EmailField(unique=True,
    help_text="Email address. Used as the unique identifier of this person."
  )
  name = models.CharField(max_length=200,
    help_text="Full name of the person"
  )
  team = models.ManyToManyField(Team, blank=True, related_name="persons", db_table="team_person",
    help_text="The team(s) the person is assigned to."
  )

  class Meta:
    db_table = "person"
    ordering = ["email"]
    verbose_name_plural = "People"

  def __str__(self):
    return self.email

# -------------------------------------------------------------------
# SourceSystem
# -------------------------------------------------------------------
class SourceSystem(AuditFields):
  short_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR], unique=True,
    help_text="Physical / concrete system identifier. Ex: 'sap1', 'sap2', 'crm', 'ga4'."
  )
  name = models.CharField(max_length=50,
    help_text="Identifying name of the source system. Does not have technical consequences."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Business description / semantic meaning of the source system."
  )
  type = models.CharField(max_length=20, choices=TYPE_CHOICES,
    help_text="System type / backend technology. Used for import and adapter logic."
  )
  include_ingest = models.CharField(max_length=20, choices=INGEST_CHOICES, default="none",
    help_text="How/if this source participates in ingestion pipelines."
  )
  generate_raw_tables = models.BooleanField(default=False,
    help_text=(
      "Default policy: create raw landing tables (TargetDatasets in schema 'raw') "
      "for all SourceDatasets in this SourceSystem."
    )
  )
  active = models.BooleanField(default=True, 
    help_text="System is still considered a live data source."
  )
  retired_at = models.DateTimeField(blank=True, null=True, 
    help_text="Automatically set when active becomes False."
  )

  class Meta:
    db_table = "source_system"
    ordering = ["short_name"]
    verbose_name_plural = "Source Systems"

  def __str__(self):
    return self.short_name

  def save(self, *args, **kwargs):
    # Automatically set retired_at when a row becomes inactive
    if not self.active and self.retired_at is None:
      self.retired_at = timezone.now()
    elif self.active:
      # If reactivated, clear retired_at
      self.retired_at = None
    super().save(*args, **kwargs)

# -------------------------------------------------------------------
# SourceDataset
# -------------------------------------------------------------------
class SourceDataset(AuditFields):
  source_system = models.ForeignKey(SourceSystem, on_delete=models.CASCADE, related_name="source_tables",
    help_text="The source system this dataset comes from."
  )
  schema_name = models.CharField(max_length=50, blank=True, null=True,
    help_text="The schema name this dataset resides on. Can be left empty if it is a default schema (eg. dbo in SQLServer)"
  )
  source_dataset_name = models.CharField(max_length=100,
    help_text="Original name of the source dataset."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional description of the business content in this dataset."
  )
  integrate = models.BooleanField(default=True,
    help_text=(
      "Controls whether this source dataset is in scope for integration into the target platform "
      "(stage/rawcore/bizcore/etc.). "
      "Set to False if the dataset is documented here but not yet ready or intentionally excluded "
      "from automated target generation."
    )
  )
  incremental = models.BooleanField(default=False,
    help_text=(
      "If True, an incremental load strategy will be applied. "
      "In this case, appropriate increment parameters have to be provided."
    )
  )
  increment_filter = models.CharField(max_length=255, blank=True, null=True,
    help_text=(
      "Template WHERE clause for incremental extraction. "
      "Use the placeholder {{DELTA_CUTOFF}} for the dynamic cutoff timestamp/date. "
      "Example: (last_update_ts >= {{DELTA_CUTOFF}} OR created_at >= {{DELTA_CUTOFF}}) "
      "AND is_deleted_flag = 0"
    )
  )
  manual_model = models.BooleanField(default=False,
    help_text="If True: dataset is manually maintained, not fully auto-generated."
  )
  distinct_select = models.BooleanField(default=False,
    help_text="If True: SELECT DISTINCT is enforced during generation."
  )
  owner = models.ManyToManyField("Person", blank=True, through="SourceDatasetOwnership", related_name="source_datasets",
    help_text="Declared business / technical owners with roles."
  )
  generate_raw_table = models.BooleanField(default=None, null=True,
    help_text=(
      "If True: force creation of a raw landing TargetDataset for this SourceDataset. "
      "If False: suppress raw landing. "
      "If None: inherit SourceSystem.generate_raw_tables."
    )
  )
  active = models.BooleanField(default=True, 
    help_text=(
      "Indicates whether this dataset is still considered an active source in the originating system. "
      "If set to False, the dataset is treated as retired (no new loads expected) but it remains "
      "in metadata for lineage, audit, and historical reference."
    )
  )
  retired_at = models.DateTimeField(blank=True, null=True,
    help_text="Auto-set when active becomes False. Used for lineage and audit."
  )

  class Meta:
    db_table = "source_dataset"
    constraints = [
      models.UniqueConstraint(
        fields=["source_system", "schema_name", "source_dataset_name"],
        name="unique_source_dataset"
      )
    ]
    ordering = ["source_system", "schema_name", "source_dataset_name"]
    verbose_name_plural = "Source Datasets"

  def __str__(self):
    return f"{display_key(self.schema_name, self.source_dataset_name)} ({self.source_system})"

  def save(self, *args, **kwargs):
    # Automatically set retired_at when a row becomes inactive
    if not self.active and self.retired_at is None:
      self.retired_at = timezone.now()
    elif self.active:
      # If reactivated, clear retired_at
      self.retired_at = None
    super().save(*args, **kwargs)

# -------------------------------------------------------------------
# SourceDatasetIncrementPolicy
# -------------------------------------------------------------------
class SourceDatasetIncrementPolicy(AuditFields):
  source_dataset = models.ForeignKey(SourceDataset, on_delete=models.CASCADE, related_name="increment_policies",
    help_text="The dataset this policy applies to."
  )
  environment = models.CharField(max_length=20, choices=ENVIRONMENT_CHOICES,
    help_text="Environment this policy applies to (e.g. dev, test, prod)."
  )
  increment_interval_length = models.PositiveIntegerField(
    help_text="How far to look back from 'now' for incremental loads in this environment."
  )
  increment_interval_unit = models.CharField(max_length=10, choices=INCREMENT_INTERVAL_CHOICES,
    help_text="Unit for the increment interval length."
  )
  active = models.BooleanField(default=True,
    help_text="If multiple rows exist (history), which one is currently active."
  )

  class Meta:
    db_table = "source_dataset_increment_policy"
    constraints = [
      models.UniqueConstraint(
        fields=["source_dataset", "environment", "active"],
        name="unique_active_increment_policy_per_env",
      )
    ]
    ordering = ["source_dataset", "environment"]

  def __str__(self):
    return f"{self.source_dataset} [{self.environment}] {self.increment_interval_length} {self.increment_interval_unit} back"
  
# -------------------------------------------------------------------
# SourceDatasetGroup
# -------------------------------------------------------------------
class SourceDatasetGroup(AuditFields):
  target_short_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR],
    help_text="Short code used to derive the unified target table prefix."
  )
  unified_source_name = models.CharField(max_length=128,
    help_text="Unified name of the source object. May be one of the source datasets which participate in this group."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional description why the group was established." 
  )
  owner = models.ManyToManyField(Person, blank=True, related_name="source_dataset_groups",
    help_text="Optional governance owner definition. May be used as default owner for generated TargetDatasets."
  )

  class Meta:
    db_table = "source_dataset_group"
    ordering = ["target_short_name", "unified_source_name"]
    verbose_name_plural = "Source Dataset Groups"

  def __str__(self):
    return f"{self.target_short_name}_{self.unified_source_name}"

# -------------------------------------------------------------------
# SourceDatasetGroupMembership
# -------------------------------------------------------------------
class SourceDatasetGroupMembership(AuditFields):
  group = models.ForeignKey(SourceDatasetGroup, on_delete=models.CASCADE, related_name="memberships",
    help_text="The source dataset group"
  )
  source_dataset = models.ForeignKey(SourceDataset, on_delete=models.CASCADE, related_name="dataset_groups",
    help_text="The dataset to be assigned to the group."
  )
  is_primary_system = models.BooleanField(default=False,
    help_text="True if this dataset is considered the 'golden' / leading source for this group."
  )

  class Meta:
    db_table = "source_dataset_group_membership"
    constraints = [models.UniqueConstraint(fields=["group", "source_dataset"], name="unique_group_membership")]
    ordering = ["-is_primary_system", "group", "source_dataset"] # - means descending

# -------------------------------------------------------------------
# SourceDatasetOwnership
# -------------------------------------------------------------------
class SourceDatasetOwnership(models.Model):
  source_dataset = models.ForeignKey("SourceDataset", on_delete=models.CASCADE, related_name="source_dataset_ownerships",
    help_text="The dataset for which an owner is declared"
  )
  person = models.ForeignKey("Person", on_delete=models.PROTECT, related_name="source_dataset_ownerships",
    help_text="The person who is declared as owner of the dataset"
  )
  role = models.CharField(max_length=20, choices=ROLE_CHOICES,
    help_text="The role which has ownership on the dataset"
  )
  is_primary = models.BooleanField(default=False,
    help_text="If True, this is the primary ownership"
  )
  since = models.DateField(blank=True, null=True,
    help_text="The date from which the ownership will start."
  )
  until = models.DateField(blank=True, null=True,
    help_text="The date on which the ownership will end."
  )
  remark = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional remarks concerning the ownership"
  )

  class Meta:
    constraints = [models.UniqueConstraint(fields=["source_dataset", "person", "role"], name="unique_source_dataset_ownership")]
    ordering = ["source_dataset", "role", "since"]

  def __str__(self):
    return f"{self.source_dataset} · {self.person} ({self.role})"

# -------------------------------------------------------------------
# SourceColumn
# -------------------------------------------------------------------
class SourceColumn(AuditFields): 
  source_dataset = models.ForeignKey(SourceDataset, on_delete=models.CASCADE, related_name="source_columns",
    help_text="The dataset this column belongs to."
  )
  source_column = models.CharField(max_length=100,
    help_text="Original source column name, eg. 'MANDT', 'VBELN'."
  )
  ordinal_position = models.PositiveIntegerField(
    help_text="Column order within the dataset."
  )
  datatype = models.CharField(max_length=20, choices=DATATYPE_CHOICES,
    help_text="Logical / normalized datatype."
  )
  max_length = models.PositiveIntegerField(blank=True, null=True,
    help_text="How many characters the field may have."
  )
  decimal_precision = models.PositiveIntegerField(blank=True, null=True,
    help_text="The decimal precision of the column."
  )
  decimal_scale = models.PositiveIntegerField(blank=True, null=True,
    help_text="The number of decimals places of the column."
  )
  nullable = models.BooleanField(default=True,
    help_text="Whether this column can be NULL in the source dataset."
  )
  primary_key_column = models.BooleanField(default=False,
    help_text="True if this column is part of the natural/business key (not the surrogate key)."
  )
  referenced_source_dataset_name = models.CharField(max_length=100, blank=True, null=True,
    help_text=(
      "Name of the upstream source object this column refers to "
      "(e.g. table 'customer_master', API 'GET /customers'). "
      "Does not have to be modeled as a SourceDataset yet. "
      "Used for lineage suggestions and future FK generation."
    )
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Business description / semantic meaning of the column."
  )
  integrate = models.BooleanField(default=False,
    help_text= (
      "Controls whether this source column is in scope for integration into the target model. "
      "Set to False if the column is not (yet) chosen for integration."
    )
  )
  pii_level = models.CharField(max_length=30, choices=PII_LEVEL_CHOICES, default="none",
    help_text="PII classification of this column, if applicable."
  )
  remark = models.CharField(max_length=255, blank=True, null=True,
    help_text="Free-form notes."
  )

  class Meta:
    db_table = "source_column"
    constraints = [models.UniqueConstraint(fields=["source_dataset", "source_column"], name="unique_source_column"),
                   models.UniqueConstraint(fields=["source_dataset", "ordinal_position"], name="unique_source_column_position")]
    ordering = ["source_dataset", "ordinal_position"]
    verbose_name_plural = "Source Columns"

  def __str__(self):
    return display_key(self.source_dataset, self.source_column)
  
# -------------------------------------------------------------------
# TargetSchema
# -------------------------------------------------------------------
class TargetSchema(AuditFields):
  short_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR], unique=True,
    help_text=(
      "Logical layer identifier. Examples: 'raw', 'stage', 'rawcore', 'bizcore', 'serving'. "
      "Defines architectural intent and default behavior."
    )
  )
  display_name = models.CharField(max_length=50, 
    help_text="Human-readable name, e.g. 'Business Core', 'Serving Layer'."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Purpose of this layer and what transformations are allowed here."
  )
  # Physical placement on the target platform
  database_name = models.CharField(max_length=100, 
    help_text="Target database / catalog on the destination platform."
  )
  schema_name = models.CharField(max_length=50, 
    help_text= (
      "Physical schema / namespace on the destination platform. "
      "Defaults to short_name, but can differ if platform naming conventions require it."
    )
  )
  # Whether end users can actively model new datasets in this layer
  is_user_visible = models.BooleanField(default=True, 
    help_text="If False, this layer is internal/technical and hidden in normal UIs."
  )
  # Default technical behavior for datasets in this schema
  default_materialization_type = models.CharField(max_length=30, choices=MATERIALIZATION_CHOICES, default="table",
    help_text="Default materialization strategy for datasets in this layer."
  )
  default_historize = models.BooleanField(default=True, 
    help_text="Whether datasets in this layer are expected to track history / SCD-style state."
  )
  # Governance defaults
  sensitivity_default = models.CharField(max_length=30, choices=SENSITIVITY_CHOICES, default="public",
    help_text="Default sensitivity classification for datasets in this schema (e.g. public, confidential)."
  )
  access_intent_default = models.CharField(max_length=30, choices=ACCESS_INTENT_CHOICES, blank=True, null=True,
    help_text="Default usage intent for governance purposes: analytics, finance_reporting, operations, ml_feature_store, etc."
  )
  # Surrogate key policy for this layer
  surrogate_keys_enabled = models.BooleanField(default=True,
    help_text=(
      "If True, this layer is expected to generate deterministic surrogate keys "
      "for its primary entities. If False, natural keys from the source are kept."
    )
  )
  surrogate_key_algorithm = models.CharField(max_length=20, default="sha256",
    help_text="Hash algorithm used for deterministic surrogate keys in this layer."
  )
  surrogate_key_null_token = models.CharField(max_length=50, default="null_replaced",
    help_text="Token to use instead of NULL in natural key components."
  )
  surrogate_key_pair_separator = models.CharField(max_length=5, default="~",
    help_text="Separator between field name and value within one component."
  )
  surrogate_key_component_separator = models.CharField(max_length=5, default="|",
    help_text="Separator between components in the natural key string."
  )
  pepper_strategy = models.CharField(max_length=50, default="runtime_pepper",
    help_text="How pepper is injected at runtime. No pepper value is stored in metadata."
  )
  is_system_managed = models.BooleanField(default=False,
    help_text="If True, this schema is managed by the system and core attributes are locked."
  )

  class Meta:
    db_table = "target_schema"
    ordering = ["short_name"]
    verbose_name_plural = "Target Schemas"

  def __str__(self):
    return f"{self.short_name} ({self.database_name}.{self.schema_name})"

# -------------------------------------------------------------------
# TargetDataset
# -------------------------------------------------------------------
class TargetDataset(AuditFields):
  # Which layer / schema this dataset belongs to
  target_schema = models.ForeignKey(TargetSchema, on_delete=models.PROTECT, related_name="target_datasets",
    help_text="Defines physical DB/schema, default materialization and governance expectations."
  )
  # Logical / business-facing name of the dataset in the target platform
  target_dataset_name = models.CharField(max_length=63, validators=[TARGET_IDENTIFIER_VALIDATOR],
    help_text="Final dataset (table/view) name, snake_case. Ex: 'sap_customer', 'sap_sales_order'."
  )
  # Incremental / historization behavior
  handle_deletes = models.BooleanField(default=True, 
    help_text="Whether deletes in the source should be reflected in this dataset."
  )
  historize = models.BooleanField(default=True,
    help_text="Track slowly changing state / valid_from / valid_to, etc."
  )
  source_datasets = models.ManyToManyField(SourceDataset, through="TargetDatasetInput", related_name="target_datasets", blank=True,
    help_text=(
      "Which source datasets feed this target dataset. "
      "Used for multi-source consolidation, staging, rawcore integration, etc."
    )
  )
  incremental_source = models.ForeignKey(SourceDataset, on_delete=models.SET_NULL, null=True, blank=True, related_name="incremental_targets",
    help_text=(
      "If set, this target dataset inherits incremental window logic (and delete detection scope) "
      "from the referenced SourceDataset. The referenced dataset's increment_filter "
      "defines which records are considered 'in scope' for delta load & delete detection."
    )
  )
  manual_model = models.BooleanField(default=False,
    help_text="If True: dataset is manually maintained, not fully auto-generated."
  )
  distinct_select = models.BooleanField(default=False,
    help_text="If True: SELECT DISTINCT is enforced during generation."
  )
  data_filter = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional row-level filter to restrict records."
  )
  partial_load = models.ManyToManyField("PartialLoad", blank=True, related_name="datasets", db_table="target_dataset_partial_load",
    help_text="Optional subset extraction definitions (per environment / window)."
  )
  owner = models.ManyToManyField("Person", blank=True, through="TargetDatasetOwnership", related_name="target_datasets",
    help_text="Declared business / technical owners with roles."
  )
  materialization_type = models.CharField(max_length=30, choices=MATERIALIZATION_CHOICES, blank=True, null=True,
    help_text=(
      "If set, overrides target_schema.default_materialization_type for this dataset. "
      "If null, dataset inherits the schema default."
    )
  )
  sensitivity = models.CharField(max_length=30, choices=SENSITIVITY_CHOICES, default="public",
    help_text="Confidentiality / sensitivity level for this dataset."
  )
  access_intent = models.CharField(max_length=30, choices=ACCESS_INTENT_CHOICES, blank=True, null=True,
    help_text="Intended usage for governance purposes, e.g. analytics, finance_reporting, ml_feature_store."
  )
  derived_from = models.ManyToManyField("self", blank=True, symmetrical=False, related_name="downstream_derivations",
    help_text=(
      "Upstream TargetDatasets that conceptually feed this dataset "
      "(e.g. rawcore entity tables feeding this bizcore table)."
    )
  )
  active = models.BooleanField(default=True, 
    help_text=(
      "If false, this dataset is deprecated. It remains in metadata for lineage "
      "and documentation but should not be generated, deployed, or used for new models."
    )
  )
  retired_at = models.DateTimeField(blank=True, null=True,
    help_text=(
      "Optional timestamp when this dataset was marked as inactive. "
      "Automatically set when active becomes False."
    )
  )
  is_system_managed = models.BooleanField(default=False,
    help_text="If True, this dataset is managed by the system and core attributes are locked."
  )

  class Meta:
    db_table = "target_dataset"
    constraints = [
      models.UniqueConstraint(
        fields=["target_schema", "target_dataset_name"],
        name="unique_target_dataset_per_schema",
      )
    ]
    ordering = ["target_dataset_name"]
    verbose_name_plural = "Target Datasets"

  def __str__(self):
    return self.target_dataset_name
  
  @property
  def effective_materialization_type(self):
    # Application-level helper, not stored in DB.
    return self.materialization_type or self.target_schema.default_materialization_type

  @property
  def natural_key_fields(self):
    """
    Returns sorted list of target_column names that are marked as primary_key_column=True.
    """
    qs = (
      self.target_columns
      .filter(primary_key_column=True)
      .values_list("target_column_name", flat=True)
    )
    return sorted(list(qs))

  def build_natural_key_string(self, record_dict):
    """
    Build the concatenated string that represents the natural key,
    BEFORE pepper and hashing.

    Example output:
    "customer_id~4711 | mandant~100"
    """
    null_token = self.target_schema.surrogate_key_null_token  # e.g. "null_replaced"
    pair_sep = self.target_schema.surrogate_key_pair_separator
    comp_sep = f" {self.target_schema.surrogate_key_component_separator} "

    parts = []
    for field in self.natural_key_fields:
      value = record_dict.get(field, null_token)
      if value is None:
        value = null_token
      parts.append(f"{field}{pair_sep}{value}")
    # join components using ' | '
    return comp_sep.join(parts)
  
  def get_runtime_pepper(self):
    """
    Load the runtime pepper. Pepper is NOT stored in metadata,
    it must come from runtime config.
    """
    pepper = os.environ.get("ELEVATA_PEPPER")
    if not pepper and hasattr(settings, "ELEVATA_PEPPER"):
      pepper = settings.ELEVATA_PEPPER
    if not pepper:
      raise ValueError(
        "No runtime pepper configured. Set ELEVATA_PEPPER in env or settings."
      )
    return pepper

  def preview_surrogate_key(self, record_dict):
    """
    Return a deterministic surrogate key (hash hex string) for a single logical record.
    This is for preview/diagnostics, not for bulk processing.
    """
    # Step 1: base natural key string (no pepper)
    base_key = self.build_natural_key_string(record_dict)

    # Step 2: add pepper
    pepper = self.get_runtime_pepper()
    key_with_pepper = f"{base_key}::{pepper}"

    # Step 3: hash using layer policy
    algo = self.target_schema.surrogate_key_algorithm  # e.g. "sha256"
    try:
      hash_func = getattr(hashlib, algo)
    except AttributeError:
      raise ValueError(f"Unsupported hash algorithm '{algo}'")

    return hash_func(key_with_pepper.encode("utf-8")).hexdigest()
  
  def save(self, *args, **kwargs):
    # Automatically set retired_at when a row becomes inactive
    if not self.active and self.retired_at is None:
      self.retired_at = timezone.now()
    elif self.active:
      # If reactivated, clear retired_at
      self.retired_at = None
    super().save(*args, **kwargs)

# -------------------------------------------------------------------
# TargetDatasetInput
# -------------------------------------------------------------------
class TargetDatasetInput(AuditFields):
  target_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="input_links",
    help_text="The stage/rawcore/bizcore dataset being built."
  )
  source_dataset = models.ForeignKey(SourceDataset, on_delete=models.CASCADE, related_name="output_links",
    help_text="The source dataset contributing to this target dataset."
  )
  role = models.CharField(max_length=50, choices=TARGET_DATASET_INPUT_ROLE_CHOICES,
    help_text=(
      "How this source contributes to the target: "
      "primary (golden source), enrichment (same entity extra attrs), "
      "reference_lookup (dim/code join), or audit_only (technical metadata)."
    ),
  )

  integration_mode = models.CharField(max_length=20, blank=True, null=True,
    help_text="How this source is integrated: 'union_all', 'merge_on_keys', 'lookup_enrichment', etc."
  )
  active = models.BooleanField(default=True,
    help_text="If false, this mapping is retained for lineage/audit but is no longer used for load."
  )

  class Meta:
    db_table = "target_dataset_input"
    constraints = [
      models.UniqueConstraint(
        fields=["target_dataset", "source_dataset"],
        name="unique_source_per_target_dataset",
      )
    ]

  def __str__(self):
    return f"{self.source_dataset} -> {self.target_dataset}"

# -------------------------------------------------------------------
# TargetDatasetOwnership
# -------------------------------------------------------------------
class TargetDatasetOwnership(models.Model):
  target_dataset = models.ForeignKey("TargetDataset", on_delete=models.CASCADE, related_name="target_dataset_ownerships",
    help_text="The dataset for which an owner is declared"
  )
  person = models.ForeignKey("Person", on_delete=models.PROTECT, related_name="target_dataset_ownerships",
    help_text="The person who has the ownership for the dataset"
  )
  role = models.CharField(max_length=20, choices=ROLE_CHOICES,
    help_text="The role which has ownership on the dataset"
  )
  is_primary = models.BooleanField(default=False,
    help_text="If True, this is the primary ownership"
  )
  since = models.DateField(blank=True, null=True,
    help_text="The date from which the ownership will start."
  )
  until = models.DateField(blank=True, null=True,
    help_text="The date on which the ownership will end."
  )
  remark = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional remarks concerning the ownership"
  )

  class Meta:
    constraints = [models.UniqueConstraint(fields=["target_dataset", "person", "role"], name="unique_target_dataset_ownership")]
    ordering = ["target_dataset", "role", "since"]

  def __str__(self):
    return f"{self.target_dataset} · {self.person} ({self.role})"

# -------------------------------------------------------------------
# TargetColumn
# -------------------------------------------------------------------
class TargetColumn(AuditFields):
  target_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="target_columns",
    help_text="The dataset this column belongs to."
  )
  target_column_name = models.CharField(max_length=63, validators=[TARGET_IDENTIFIER_VALIDATOR],
    help_text="Final column name in snake_case. Ex: 'customer_name', 'order_created_tms'."
  )
  ordinal_position = models.PositiveIntegerField(
    help_text="Column order within the dataset."
  )
  source_columns = models.ManyToManyField("SourceColumn", through="TargetColumnInput", related_name="mapped_target_columns", blank=True,
    help_text="Which source columns contribute to this target column."
  )
  datatype = models.CharField(max_length=20, choices=DATATYPE_CHOICES,
    help_text="Logical / normalized datatype."
  )
  max_length = models.PositiveIntegerField(blank=True, null=True,
    help_text="How many characters the field may have."
  )
  decimal_precision = models.PositiveIntegerField(blank=True, null=True,
    help_text="The decimal precision of the column."
  )
  decimal_scale = models.PositiveIntegerField(blank=True, null=True,
    help_text="The number of decimals places of the column."
  )
  nullable = models.BooleanField(default=True,
    help_text="Whether this column can be NULL in the final target dataset."
  )
  # Important: this is NOT the surrogate PK.
  # This flags business key components or domain-identifying columns.
  primary_key_column = models.BooleanField(default=False,
    help_text="True if this column is part of the natural/business key (not the surrogate key)."
  )
  artificial_column = models.BooleanField(default=False,
    help_text=(
      "True if this column does not directly exist in any single source, "
      "but is computed / harmonized / derived."
    )
  )
  manual_expression = models.TextField(blank=True, null=True,
    help_text=(
      "Default platform-neutral expression (elevata DSL) for deriving this column. "
      "Example: {{ UPPER(customer_name) }}, {{ DATE_ADD('day', order_date, 7) }}, "
      "{{ COALESCE(a, b, 'fallback') }}. "
      "This applies to all inputs unless overridden at source level."
    )
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Business description / semantic meaning of the column."
  )
  pii_level = models.CharField(max_length=30, choices=PII_LEVEL_CHOICES, default="none",
    help_text="PII classification of this column, if applicable."
  )
  remark = models.CharField(max_length=255, blank=True, null=True,
    help_text="Free-form notes."
  )
  sensitivity = models.CharField(max_length=30, choices=SENSITIVITY_CHOICES, default="public",
    help_text="Sensitivity classification for this column (e.g. public, confidential, restricted)."
  )
  lineage_origin = models.CharField(max_length=20, choices=LINEAGE_ORIGIN_CHOICES, default="direct",
    help_text="How this column is derived: direct source field, derived calculation, lookup join, constant, etc."
  )
  profiling_stats = models.JSONField(blank=True, null=True, 
    help_text=(
      "Optional summarized profiling statistics for this column. "
      "Intended for descriptive metadata only (e.g., null rate, distinct count, top values)."
    )
  )
  active = models.BooleanField(default=True, 
    help_text=(
      "If false, this column is deprecated. It remains in metadata for lineage "
      "and documentation but should not be generated, deployed, or used for new models."
    )
  )
  retired_at = models.DateTimeField(blank=True, null=True,
    help_text=(
      "Optional timestamp when this column was marked as inactive. "
      "Automatically set when active becomes False."
    )
  )
  is_system_managed = models.BooleanField(default=False,
    help_text="If True, this column is managed by the system and core attributes are locked."
  )

  class Meta:
    db_table = "target_column"
    constraints = [
      models.UniqueConstraint(
        fields=["target_dataset", "target_column_name"],
        name="unique_target_column"
      ),
      models.UniqueConstraint(
        fields=["target_dataset", "ordinal_position"],
        name="unique_target_column_position"
      ),
    ]
    ordering = ["target_dataset", "ordinal_position"]
    verbose_name_plural = "Target Columns"

  def __str__(self):
    return display_key(self.target_dataset, self.target_column_name)

  def save(self, *args, **kwargs):
    # handle retired_at
    if not self.active and self.retired_at is None:
      self.retired_at = timezone.now()
    elif self.active:
      self.retired_at = None
    super().save(*args, **kwargs)

# -------------------------------------------------------------------
# TargetColumnInput
# -------------------------------------------------------------------
class TargetColumnInput(models.Model):
  target_column = models.ForeignKey(TargetColumn, on_delete=models.CASCADE, related_name="input_links",
    help_text="The target column being populated."
  )
  source_column = models.ForeignKey(SourceColumn, on_delete=models.PROTECT, related_name="output_links",
    help_text="The specific source column feeding this target column."
  )
  manual_expression = models.TextField(blank=True, null=True,
    help_text=(
      "Optional platform-neutral expression (elevata DSL) that applies "
      "ONLY when using this specific source column. "
      "If empty, the TargetColumn.manual_expression is used."
    )
  )
  ordinal_position = models.PositiveIntegerField(default=1, 
    help_text=(
      "Priority / fallback order when multiple source systems "
      "feed the same target column. 1 = highest priority."
    )
  )
  active = models.BooleanField(default=True,
    help_text="If false, kept for lineage/history but ignored going forward."
  )

  class Meta:
    db_table = "target_column_input"
    constraints = [
      models.UniqueConstraint(
        fields=["target_column", "source_column"],
        name="unique_target_column_input_source"
      )
    ]
    ordering = ["target_column", "ordinal_position"]

  def __str__(self):
    return f"{self.source_column} -> {self.target_column} (#{self.ordinal_position})"
  
# -------------------------------------------------------------------
# IncrementFieldMap
# -------------------------------------------------------------------
class IncrementFieldMap(AuditFields):
  """
  Maps the field names used in the source-level increment_filter
  to the equivalent columns in this target dataset.
  This lets us reuse the same logical filter for delete detection in core.
  """
  target_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="incremental_field_mappings",
    help_text="The core/target dataset that consumes this mapping."
  )
  source_field_name = models.CharField(max_length=100, 
    help_text=(
      "Field name as it appears in the source dataset's increment_filter "
      "(e.g. 'last_update_ts', 'created_at', 'is_deleted_flag')."
    )
  )
  target_column = models.ForeignKey(TargetColumn, on_delete=models.CASCADE, related_name="used_for_incremental_filter",
    help_text=(
      "Column in this TargetDataset that semantically corresponds "
      "to that source field."
    )
  )

  class Meta:
    db_table = "incremental_field_mapping"
    constraints = [
      models.UniqueConstraint(
        fields=["target_dataset", "source_field_name"],
        name="unique_incremental_field_mapping_per_target",
      )
    ]

  def __str__(self):
    return f"{self.target_dataset}.{self.source_field_name} -> {self.target_column.target_column_name}"

# -------------------------------------------------------------------
# TargetDatasetReference
# -------------------------------------------------------------------
class TargetDatasetReference(AuditFields):
  referencing_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="outgoing_references",
    help_text="Child dataset that holds the foreign key."
  )
  referenced_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="incoming_references",
    help_text="Parent dataset that defines the business entity / surrogate PK."
  )
  reference_prefix = models.CharField(max_length=10, blank=True, null=True, validators=[SHORT_NAME_VALIDATOR],
    help_text=(
      "If provided, forms the FK name in the child as <prefix>_<referenced_dataset>_key. "
      "Example: prefix 'billing' + referenced 'sap_customer' -> 'billing_sap_customer_key'."
    )
  )
  relationship_type = models.CharField(max_length=20, choices=RELATIONSHIP_TYPE_CHOICES, default="n_to_1",
    help_text="Type of relationship of this reference."
  )
  join_condition_hint = models.CharField(max_length=255, blank=True, null=True,
    help_text=(
      "Human/machine-readable join hint for lineage visualization. "
      "Example: 'child.billing_sap_customer_key = parent.sap_customer_key'."
    )
  )

  class Meta:
    db_table = "target_dataset_reference"
    constraints = [
      models.UniqueConstraint(
        fields=["referencing_dataset", "reference_prefix", "referenced_dataset"],
        name="unique_target_dataset_reference"
      )
    ]
    ordering = ["referencing_dataset", "reference_prefix", "referenced_dataset"]
    verbose_name_plural = "Target Dataset References"

  def __str__(self):
    return f"{self.referencing_dataset} -> {self.referenced_dataset} ({self.reference_prefix or ''})"

# -------------------------------------------------------------------
# TargetDatasetReferenceComponent
# -------------------------------------------------------------------
class TargetDatasetReferenceComponent(AuditFields):
  reference = models.ForeignKey(TargetDatasetReference, on_delete=models.CASCADE, related_name="key_components",
    help_text="The parent-child relationship this mapping belongs to."
  )
  from_column = models.ForeignKey(TargetColumn, on_delete=models.PROTECT, related_name="fk_components_outgoing",
    help_text="Column on the referencing (child) dataset."
  )
  to_column = models.ForeignKey(TargetColumn, on_delete=models.PROTECT, related_name="fk_components_incoming",
    help_text="Column on the referenced (parent) dataset."
  )
  ordinal_position = models.PositiveIntegerField(default=1,
    help_text="Order for composite keys."
  )

  class Meta:
    db_table = "target_reference_key_component"
    constraints = [
      models.UniqueConstraint(
        fields=["reference", "from_column", "to_column"],
        name="unique_reference_parent_key_component"
      )
    ]
    ordering = ["reference", "ordinal_position"]
    verbose_name_plural = "Target Reference Components"

  def __str__(self):
    return f"{self.reference.referencing_dataset}.{self.from_column.target_column_name} -> {self.reference.referenced_dataset}.{self.to_column.target_column_name} (#{self.ordinal_position})"
