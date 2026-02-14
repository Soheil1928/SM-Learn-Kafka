from rest_framework import serializers
from .models import Book


class BookSerializer(serializers.ModelSerializer):
    class Meta:
        model = Book
        fields = ['id', 'title', 'author', 'isbn', 'published_year', 'price', 'created_by', 'created_at', 'updated_at']
        read_only_fields = ['created_at', 'updated_at']

    # اعتبارسنجی سفارشی برای ISBN
    def validate_isbn(self, value):
        """بررسی فرمت ISBN (ساده شده)"""
        if len(value) not in [10, 13]:
            raise serializers.ValidationError("ISBN must be either 10 or 13 characters long")
        if not value.isdigit():
            raise serializers.ValidationError("ISBN must contain only digits")
        return value

    # اعتبارسنجی برای سال انتشار
    def validate_published_year(self, value):
        """بررسی معتبر بودن سال انتشار"""
        import datetime
        current_year = datetime.datetime.now().year
        if value < 1800 or value > current_year + 5:  # حداکثر 5 سال آینده
            raise serializers.ValidationError(f"Published year must be between 1800 and {current_year + 5}")
        return value

    # اعتبارسنجی برای قیمت
    def validate_price(self, value):
        """بررسی مثبت بودن قیمت"""
        if value <= 0:
            raise serializers.ValidationError("Price must be greater than zero")
        return value