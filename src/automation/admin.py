from django.contrib import admin

from .models import Job

@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    filters = ("type_key", "status", "run_at")
