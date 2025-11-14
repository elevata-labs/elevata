from django.contrib import admin
from metadata.ingestion.import_service import import_metadata_for_datasets
from metadata.models import TargetDataset, TargetColumn

admin.site.register(TargetDataset)
admin.site.register(TargetColumn)
