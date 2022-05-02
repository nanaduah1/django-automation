from django.contrib import admin

from .models import Job
from .models import Message

admin.site.register(Message)


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    filters = ("type_key", "status", "run_at")
