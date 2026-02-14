from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.conf import settings

from .models import Book
from .serializers import BookSerializer
from .kafka_producer import kafka_producer


@api_view(['GET', 'POST'])
def book_list(request):
    if request.method == 'GET':
        books = Book.objects.all()
        serializer = BookSerializer(books, many=True)
        return Response(serializer.data)

    elif request.method == 'POST':
        serializer = BookSerializer(data=request.data)
        if serializer.is_valid():
            book = serializer.save()

            # ارسال رویداد به کافکا
            event_data = {
                'id': book.id,
                'title': book.title,
                'author': book.author,
                'isbn': book.isbn,
                'published_year': book.published_year,
                'price': str(book.price),
                'created_by': book.created_by,
                'action': 'create',
                'timestamp': str(book.created_at)
            }

            kafka_producer.send_message(
                topic=settings.KAFKA_TOPIC_BOOK_CREATED,
                key=book.id,
                value=event_data
            )
            kafka_producer.flush()

            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET', 'PUT', 'DELETE'])
def book_detail(request, pk):
    book = get_object_or_404(Book, pk=pk)

    if request.method == 'GET':
        serializer = BookSerializer(book)
        return Response(serializer.data)

    elif request.method == 'PUT':
        serializer = BookSerializer(book, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    elif request.method == 'DELETE':
        book.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)