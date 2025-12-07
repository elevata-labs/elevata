from django.apps import AppConfig

class MetadataConfig(AppConfig):
  default_auto_field = "django.db.models.BigAutoField"
  name = "metadata"
  label = "metadata"
  verbose_name = "Metadata"

  def ready(self) -> None:
    # Import signal handlers to connect them
    from . import signals  # noqa: F401
