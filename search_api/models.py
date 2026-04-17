from django.db import models


class SearchAnalytics(models.Model):
    SEARCH_TYPE_CHOICES = [
        ('basic', 'Basic Search'),
        ('advanced', 'Advanced Search'),
    ]

    DEVICE_TYPE_CHOICES = [
        ('desktop', 'Desktop'),
        ('mobile', 'Mobile'),
        ('tablet', 'Tablet'),
        ('bot', 'Bot / Crawler'),
        ('unknown', 'Unknown'),
    ]

    # --- Who ---
    ip_address  = models.GenericIPAddressField(db_index=True)
    user_agent  = models.TextField(blank=True)
    browser     = models.CharField(max_length=120, blank=True)
    os          = models.CharField(max_length=120, blank=True)
    device_type = models.CharField(max_length=20, choices=DEVICE_TYPE_CHOICES, default='unknown')

    # --- Where (from IP geolocation) ---
    city        = models.CharField(max_length=120, blank=True)
    region      = models.CharField(max_length=120, blank=True)
    country     = models.CharField(max_length=120, blank=True)
    country_code = models.CharField(max_length=4, blank=True)
    latitude    = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitude   = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    isp         = models.CharField(max_length=255, blank=True)
    is_proxy    = models.BooleanField(default=False)
    is_hosting  = models.BooleanField(default=False)

    # --- What was searched ---
    search_type    = models.CharField(max_length=20, choices=SEARCH_TYPE_CHOICES, db_index=True)
    query          = models.TextField(blank=True)
    court_location = models.CharField(max_length=255, blank=True)
    results_count  = models.IntegerField(default=0)
    latency_ms     = models.IntegerField(default=0)

    # --- When ---
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ['-timestamp']
        verbose_name = 'Search Analytics Record'
        verbose_name_plural = 'Search Analytics'
        indexes = [
            models.Index(fields=['ip_address', 'timestamp']),
            models.Index(fields=['country', 'timestamp']),
        ]

    def __str__(self):
        return f"{self.ip_address} [{self.search_type}] @ {self.timestamp:%Y-%m-%d %H:%M:%S}"
