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

from django.conf import settings
from django.db import models
from django.core.validators import RegexValidator
from crum import get_current_user
from generic import display_key
from metadata.constants import TYPE_CHOICES, INGEST_CHOICES, LAYER_CHOICES, INTERVAL_CHOICES, DATATYPE_CHOICES, ROLE_CHOICES 

NAME_VALIDATOR = RegexValidator(r"^[a-zA-Z0-9_.-]+$", "Only a–z, 0–9, _, ., - allowed.")
SHORT_NAME_VALIDATOR = RegexValidator(r"^[a-z]+$", "Only a–z allowed.")

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

# Metadata models
class PartialLoad(AuditFields):
  name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR], unique=True)
  description = models.CharField(max_length=255, blank=True, null=True)

  class Meta:
    db_table = "partial_load"
    ordering = ["name"]
    verbose_name_plural = "Partial Loads"

  def __str__(self):
    return self.name

class Team(AuditFields):
  name = models.CharField(max_length=30, unique=True)
  description = models.CharField(max_length=255, blank=True, null=True)

  class Meta:
    db_table = "team"
    ordering = ["name"]
    verbose_name_plural = "Teams"

  def __str__(self):
    return self.name

class Person(AuditFields):
  email = models.EmailField(unique=True)
  name = models.CharField(max_length=200, unique=True)
  team = models.ManyToManyField("Team", blank=True, related_name="persons", db_table="team_person")

  class Meta:
    db_table = "person"
    ordering = ["email"]
    verbose_name_plural = "People"

  def __str__(self):
    return self.email

class SourceSystem(AuditFields):
  short_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR], unique=True)
  name = models.CharField(max_length=50)
  description = models.CharField(max_length=255, blank=True, null=True)
  type = models.CharField(max_length=20, choices=TYPE_CHOICES)
  target_short_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR])
  include_ingest = models.CharField(max_length=20, choices=INGEST_CHOICES, default="(none)") 
  generate_raw_layer = models.BooleanField(default=False)
  raw_database = models.CharField(max_length=100, default='"{{ target.database }}"')
  raw_schema = models.CharField(max_length=30, default="raw")
  increment_where = models.CharField(max_length=5, choices=LAYER_CHOICES, default="stage")

  class Meta:
    db_table = "source_system"
    ordering = ["short_name"]
    verbose_name_plural = "Source Systems"

  def __str__(self):
    return self.short_name

class SourceDataset(AuditFields):
  source_system = models.ForeignKey(SourceSystem, on_delete=models.CASCADE, related_name="source_tables")
  schema = models.CharField(max_length=50, blank=True, null=True)
  source_dataset = models.CharField(max_length=100)
  description = models.CharField(max_length=255, blank=True, null=True)
  get_metadata = models.BooleanField(default=True)
  integrate = models.BooleanField(default=True)
  stage_dataset = models.CharField(max_length=100, blank=True, null=True)
  incremental = models.BooleanField(default=False)
  increment_filter = models.CharField(max_length=255, blank=True, null=True)
  increment_interval = models.CharField(max_length=10, choices=INTERVAL_CHOICES, blank=True, null=True)
  interval_length_dev = models.PositiveIntegerField(blank=True, null=True)
  interval_length_test = models.PositiveIntegerField(blank=True, null=True)
  interval_length_prod = models.PositiveIntegerField(blank=True, null=True)
  manual_maintained_model = models.BooleanField(default=False)
  distinct_select = models.BooleanField(default=False)
  owner = models.ManyToManyField(Person, through="SourceDatasetOwnership", related_name="source_datasets")

  class Meta:
    db_table = "source_dataset"
    constraints = [models.UniqueConstraint(fields=["source_system", "schema", "source_dataset"], name="unique_source_dataset")]
    ordering = ["source_system", "schema", "source_dataset"]
    verbose_name_plural = "Source Datasets"

  def __str__(self):
    return f"{display_key(self.schema, self.source_dataset)} ({self.source_system})"


class SourceDatasetOwnership(models.Model):
  source_dataset = models.ForeignKey("SourceDataset", on_delete=models.CASCADE, related_name="source_dataset_ownerships")
  person = models.ForeignKey("Person", on_delete=models.PROTECT, related_name="source_dataset_ownerships")
  role = models.CharField(max_length=20, choices=ROLE_CHOICES)
  is_primary = models.BooleanField(default=False)
  since = models.DateField(blank=True, null=True)
  until = models.DateField(blank=True, null=True)
  notes = models.CharField(max_length=255, blank=True, null=True)

  class Meta:
    constraints = [models.UniqueConstraint(fields=["source_dataset", "person", "role"], name="unique_source_dataset_ownership")]
    ordering = ["source_dataset", "role", "since"]

  def __str__(self):
    return f"{self.source_dataset} · {self.person} ({self.role})"

class SourceColumn(AuditFields): 
  source_dataset = models.ForeignKey(SourceDataset, on_delete=models.CASCADE, related_name="source_columns")
  source_column = models.CharField(max_length=100)
  ordinal_position = models.PositiveIntegerField()
  datatype = models.CharField(max_length=20, choices=DATATYPE_CHOICES)
  max_length = models.PositiveIntegerField(blank=True, null=True)
  decimal_precision = models.PositiveIntegerField(blank=True, null=True)
  decimal_scale = models.PositiveIntegerField(blank=True, null=True)
  nullable = models.BooleanField(default=True)
  primary_key_column = models.BooleanField(default=False)
  referenced_source_dataset = models.CharField(max_length=100, blank=True, null=True)
  description = models.CharField(max_length=255, blank=True, null=True)
  integrate = models.BooleanField(default=False)
  pii_column = models.BooleanField(default=False)
  remark = models.CharField(max_length=255, blank=True, null=True)

  class Meta:
    db_table = "source_column"
    constraints = [models.UniqueConstraint(fields=["source_dataset", "source_column"], name="unique_source_column"),
                   models.UniqueConstraint(fields=["source_dataset", "ordinal_position"], name="unique_source_column_position")]
    ordering = ["source_dataset", "ordinal_position"]
    verbose_name_plural = "Source Columns"

  def __str__(self):
    return display_key(self.source_dataset, self.source_column)

class TargetDataset(AuditFields):
  target_dataset = models.CharField(max_length=100, unique=True)
  stage_dataset = models.CharField(max_length=100)
  handle_deletes = models.BooleanField(default=True)
  core_increment_filter = models.CharField(max_length=255, blank=True, null=True)
  historize = models.BooleanField(default=True)
  manual_maintained_model = models.BooleanField(default=False)
  distinct_select = models.BooleanField(default=False)
  data_filter = models.CharField(max_length=255, blank=True, null=True)
  partial_load = models.ManyToManyField("PartialLoad", blank=True, related_name="datasets", db_table="target_dataset_partial_load")
  owner = models.ManyToManyField(Person, through="TargetDatasetOwnership", related_name="target_datasets")

  class Meta:
    db_table = "target_dataset"
    ordering = ["target_dataset"]
    verbose_name_plural = "Target Datasets"

  def __str__(self):
    return self.target_dataset

class TargetDatasetOwnership(models.Model):
  target_dataset = models.ForeignKey("TargetDataset", on_delete=models.CASCADE, related_name="target_dataset_ownerships")
  person = models.ForeignKey("Person", on_delete=models.PROTECT, related_name="target_dataset_ownerships")
  role = models.CharField(max_length=20, choices=ROLE_CHOICES)
  is_primary = models.BooleanField(default=False)
  since = models.DateField(blank=True, null=True)
  until = models.DateField(blank=True, null=True)
  notes = models.CharField(max_length=255, blank=True, null=True)

  class Meta:
    constraints = [models.UniqueConstraint(fields=["target_dataset", "person", "role"], name="unique_target_dataset_ownership")]
    ordering = ["target_dataset", "role", "since"]

  def __str__(self):
    return f"{self.target_dataset} · {self.person} ({self.role})"

class TargetColumn(AuditFields):  
  target_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="target_columns")
  target_column = models.CharField(max_length=100)
  source_column = models.ForeignKey(SourceColumn, on_delete=models.CASCADE, blank=True, null=True, related_name="target_columns")
  ordinal_position = models.PositiveIntegerField()
  datatype = models.CharField(max_length=20, choices=DATATYPE_CHOICES)
  max_length = models.PositiveIntegerField(blank=True, null=True)
  decimal_precision = models.PositiveIntegerField(blank=True, null=True)
  decimal_scale = models.PositiveIntegerField(blank=True, null=True)
  nullable = models.BooleanField(default=True)
  primary_key_column = models.BooleanField(default=False)
  artificial_column = models.BooleanField(default=False)
  manual_expression = models.CharField(max_length=255, blank=True, null=True)
  description = models.CharField(max_length=255, blank=True, null=True)
  integrate = models.BooleanField(default=True)
  pii_column = models.BooleanField(default=False)
  remark = models.CharField(max_length=255, blank=True, null=True)

  class Meta:
    db_table = "target_column"
    constraints = [models.UniqueConstraint(fields=["target_dataset", "target_column"], name="unique_target_column"),
                   models.UniqueConstraint(fields=["target_dataset", "ordinal_position"], name="unique_target_column_position")]
    ordering = ["target_dataset", "ordinal_position"]
    verbose_name_plural = "Target Columns"

  def __str__(self):
    return display_key(self.target_dataset, self.target_column)

class TargetDatasetReference(AuditFields):
  target_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="relations")
  reference_prefix = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR])
  target_column = models.ForeignKey(TargetColumn, on_delete=models.CASCADE, related_name="relations")
  referenced_target_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="referenced_relations")
  referenced_target_column = models.ForeignKey(TargetColumn, on_delete=models.CASCADE, related_name="referenced_relations")

  class Meta:
    db_table = "target_dataset_relation"
    constraints = [models.UniqueConstraint(fields=["target_dataset", "reference_prefix", "target_column"], name="unique_target_dataset_relation")]
    ordering = ["target_dataset", "reference_prefix", "target_column"]
    verbose_name_plural = "Target Dataset References"