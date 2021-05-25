from django.db import models

# Create your models here.

class Poll(models.Model):

    question= models.TextField()
    option_one= models.CharField(max_length=50)
    option_two=models.CharField(max_length=50)
    option_three=models.CharField(max_length=50)
    option_one_count=models.IntegerField(default=0)
    option_two_count= models.IntegerField(default=0)
    option_three_count=models.IntegerField(default=0)
    
   # total_count =option_one_count+option_two_count+option_three_count
    def total(self):
        return self.option_one_count+self.option_two_count+self.option_three_count

# class Results(models.Model):

#     question= models.TextField()
#     option_one= models.CharField(max_length=50)
#     option_two=models.CharField(max_length=50)
#     option_three=models.CharField(max_length=50)
#     option_one_count=models.IntegerField(default=0)
#     option_two_count= models.IntegerField(default=0)
#     option_three_count=models.IntegerField(default=0)

#        # total_count =option_one_count+option_two_count+option_three_count
#     def total(self):
#         return self.option_one_count+self.option_two_count+self.option_three_count
    
    