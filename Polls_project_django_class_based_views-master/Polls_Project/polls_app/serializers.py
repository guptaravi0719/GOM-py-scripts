from rest_framework import serializers

from .models import Poll


class PollSerializer(serializers.ModelSerializer):
    id = serializers.ReadOnlyField()
    question = serializers.ReadOnlyField()
    option_one = serializers.ReadOnlyField()
    option_two = serializers.ReadOnlyField()
    option_three = serializers.ReadOnlyField()




    class Meta:
        model = Poll
        fields = ['id', 'question', 'option_one', 'option_two', 'option_three']


class VoteViewOptionSerializer(serializers.ModelSerializer):
    id = serializers.ReadOnlyField()
    question= serializers.ReadOnlyField()
    option_one = serializers.ReadOnlyField()
    option_two = serializers.ReadOnlyField()
    option_three = serializers.ReadOnlyField()

    class Meta:
        model = Poll

        fields = ['id','question', 'option_one', 'option_two', 'option_three']


class ResultSerializer(serializers.ModelSerializer):
    question = serializers.ReadOnlyField()
    option_one = serializers.ReadOnlyField()
    option_two = serializers.ReadOnlyField()
    option_three = serializers.ReadOnlyField()
    option_one_count = serializers.ReadOnlyField()
    option_two_count = serializers.ReadOnlyField()
    option_three_count = serializers.ReadOnlyField()
    total_count = serializers.ReadOnlyField()

    class Meta:
        model = Poll

        fields = ['id', 'question','option_one', 'option_two', 'option_three', 'option_one_count', 'option_two_count', 'option_three_count', 'total_count']
