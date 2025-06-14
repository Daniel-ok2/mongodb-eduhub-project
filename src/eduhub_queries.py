from pymongo import MongoClient

# Connect to MongoDB ( connection string)
client = MongoClient("mongodb://localhost:27017/")
db = client["eduhub"]
courses_collection = db["courses"]

# Part 1: Basic Queries
def get_all_courses():
    """Retrieve all courses from the courses collection."""
    return list(courses_collection.find())

def get_course_by_id(course_id):
    """Retrieve a course by its ID."""
    return courses_collection.find_one({"_id": course_id})

# Placeholder for Part 2: Advanced Queries
# Add more complex queries later (e.g., aggregations, joins)

if __name__ == "__main__":
    # Test queries
    print(get_all_courses())

# Import required libraries
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from bson import ObjectId
from pymongo.errors import DuplicateKeyError

# Establish MongoDB connection
client = MongoClient('mongodb://localhost:27017/')  # Update with Atlas URI if needed
db = client['eduhub_db']

# Drop existing collections for a clean setup (optional, remove for production)
for collection in db.list_collection_names():
    db[collection].drop()

# Define and create collections with validation rules
# Users collection
db.create_collection('users', validator={
    '$jsonSchema': {
        'bsonType': 'object',
        'required': ['userId', 'email', 'firstName', 'lastName', 'role', 'dateJoined', 'isActive'],
        'properties': {
            'userId': {'bsonType': 'string'},
            'email': {'bsonType': 'string', 'pattern': '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'},
            'firstName': {'bsonType': 'string'},
            'lastName': {'bsonType': 'string'},
            'role': {'enum': ['student', 'instructor']},
            'dateJoined': {'bsonType': 'date'},
            'profile': {
                'bsonType': 'object',
                'properties': {
                    'bio': {'bsonType': 'string'},
                    'avatar': {'bsonType': 'string'},
                    'skills': {'bsonType': 'array', 'items': {'bsonType': 'string'}}
                }
            },
            'isActive': {'bsonType': 'bool'}
        }
    }
})

# Courses collection
db.create_collection('courses', validator={
    '$jsonSchema': {
        'bsonType': 'object',
        'required': ['courseId', 'title', 'instructorId', 'category', 'level', 'price', 'createdAt', 'isPublished'],
        'properties': {
            'courseId': {'bsonType': 'string'},
            'title': {'bsonType': 'string'},
            'description': {'bsonType': 'string'},
            'instructorId': {'bsonType': 'string'},
            'category': {'bsonType': 'string'},
            'level': {'enum': ['beginner', 'intermediate', 'advanced']},
            'duration': {'bsonType': 'double'},
            'price': {'bsonType': 'double'},
            'tags': {'bsonType': 'array', 'items': {'bsonType': 'string'}},
            'createdAt': {'bsonType': 'date'},
            'updatedAt': {'bsonType': 'date'},
            'isPublished': {'bsonType': 'bool'}
        }
    }
})

# Enrollments collection
db.create_collection('enrollments', validator={
    '$jsonSchema': {
        'bsonType': 'object',
        'required': ['userId', 'courseId', 'enrolledAt'],
        'properties': {
            'userId': {'bsonType': 'string'},
            'courseId': {'bsonType': 'string'},
            'enrolledAt': {'bsonType': 'date'},
            'progress': {'bsonType': 'double'},  # Percentage completed
            'completed': {'bsonType': 'bool'}
        }
    }
})

# Lessons collection
db.create_collection('lessons', validator={
    '$jsonSchema': {
        'bsonType': 'object',
        'required': ['lessonId', 'courseId', 'title', 'createdAt'],
        'properties': {
            'lessonId': {'bsonType': 'string'},
            'courseId': {'bsonType': 'string'},
            'title': {'bsonType': 'string'},
            'content': {'bsonType': 'string'},
            'duration': {'bsonType': 'double'},  # In minutes
            'createdAt': {'bsonType': 'date'}
        }
    }
})

# Assignments collection
db.create_collection('assignments', validator={
    '$jsonSchema': {
        'bsonType': 'object',
        'required': ['assignmentId', 'courseId', 'title', 'dueDate'],
        'properties': {
            'assignmentId': {'bsonType': 'string'},
            'courseId': {'bsonType': 'string'},
            'title': {'bsonType': 'string'},
            'description': {'bsonType': 'string'},
            'dueDate': {'bsonType': 'date'},
            'maxScore': {'bsonType': 'double'}
        }
    }
})

# Submissions collection
db.create_collection('submissions', validator={
    '$jsonSchema': {
        'bsonType': 'object',
        'required': ['submissionId', 'assignmentId', 'userId', 'submittedAt'],
        'properties': {
            'submissionId': {'bsonType': 'string'},
            'assignmentId': {'bsonType': 'string'},
            'userId': {'bsonType': 'string'},
            'content': {'bsonType': 'string'},
            'submittedAt': {'bsonType': 'date'},
            'grade': {'bsonType': 'double'},
            'feedback': {'bsonType': 'string'}
        }
    }
})

# Ensure unique indexes to prevent duplicates
db.users.create_index('userId', unique=True)
db.users.create_index('email', unique=True)
db.courses.create_index('courseId', unique=True)
db.enrollments.create_index([('userId', 1), ('courseId', 1)], unique=True)
db.lessons.create_index('lessonId', unique=True)
db.assignments.create_index('assignmentId', unique=True)
db.submissions.create_index('submissionId', unique=True)

print("Database 'eduhub_db' and collections created with validation rules and indexes.")


# Import libraries
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import json
import uuid

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['eduhub_db']

# Helper function to generate unique IDs
def generate_id(prefix):
    return f"{prefix}{str(uuid.uuid4())[:8]}"

# Task 2.1: Insert Sample Data

# Insert 20 users (15 students, 5 instructors)
users = [
    {
        "userId": generate_id("U"),
        "email": f"student{i}@eduhub.com",
        "firstName": f"Student{i}",
        "lastName": "Doe",
        "role": "student",
        "dateJoined": datetime.now() - timedelta(days=i*30),
        "profile": {"bio": f"Learning enthusiast #{i}", "avatar": f"avatar{i}.png", "skills": ["Python", "SQL"]},
        "isActive": True
    } for i in range(1, 16)
] + [
    {
        "userId": generate_id("U"),
        "email": f"instructor{i}@eduhub.com",
        "firstName": f"Instructor{i}",
        "lastName": "Smith",
        "role": "instructor",
        "dateJoined": datetime.now() - timedelta(days=i*60),
        "profile": {"bio": f"Expert in data education #{i}", "avatar": f"avatar{i}.png", "skills": ["MongoDB", "Data Engineering"]},
        "isActive": True
    } for i in range(1, 6)
]
db.users.insert_many(users)
print("Inserted 20 users")

# Insert 8 courses across categories
instructor_ids = [u["userId"] for u in db.users.find({"role": "instructor"})]
courses = [
    {
        "courseId": generate_id("C"),
        "title": "Intro to Data Engineering",
        "description": "Learn data pipelines and databases",
        "instructorId": instructor_ids[0],
        "category": "Data Engineering",
        "level": "beginner",
        "duration": 10.5,
        "price": 99.99,
        "tags": ["data", "pipelines"],
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "isPublished": True
    },
    {
        "courseId": generate_id("C"),
        "title": "Advanced Python Programming",
        "description": "Master Python for data science",
        "instructorId": instructor_ids[1],
        "category": "Python",
        "level": "intermediate",
        "duration": 8.0,
        "price": 149.99,
        "tags": ["python", "coding"],
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "isPublished": True
    },
    {
        "courseId": generate_id("C"),
        "title": "Machine Learning Basics",
        "description": "Introduction to AI and ML",
        "instructorId": instructor_ids[2],
        "category": "AI",
        "level": "beginner",
        "duration": 12.0,
        "price": 199.99,
        "tags": ["ai", "machine learning"],
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "isPublished": False
    },
    {
        "courseId": generate_id("C"),
        "title": "Web Development with JavaScript",
        "description": "Build modern web apps",
        "instructorId": instructor_ids[3],
        "category": "Web Development",
        "level": "intermediate",
        "duration": 15.0,
        "price": 129.99,
        "tags": ["javascript", "web"],
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "isPublished": True
    },
    {
        "courseId": generate_id("C"),
        "title": "SQL for Data Analysis",
        "description": "Query databases effectively",
        "instructorId": instructor_ids[4],
        "category": "Data Analysis",
        "level": "beginner",
        "duration": 6.0,
        "price": 79.99,
        "tags": ["sql", "data"],
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "isPublished": True
    },
    {
        "courseId": generate_id("C"),
        "title": "Big Data with Hadoop",
        "description": "Handle large-scale data",
        "instructorId": instructor_ids[0],
        "category": "Data Engineering",
        "level": "advanced",
        "duration": 20.0,
        "price": 249.99,
        "tags": ["hadoop", "big data"],
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "isPublished": True
    },
    {
        "courseId": generate_id("C"),
        "title": "Deep Learning with TensorFlow",
        "description": "Advanced neural networks",
        "instructorId": instructor_ids[1],
        "category": "AI",
        "level": "advanced",
        "duration": 18.0,
        "price": 299.99,
        "tags": ["tensorflow", "deep learning"],
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "isPublished": False
    },
    {
        "courseId": generate_id("C"),
        "title": "React for Front-End Development",
        "description": "Build dynamic UIs",
        "instructorId": instructor_ids[2],
        "category": "Web Development",
        "level": "intermediate",
        "duration": 10.0,
        "price": 159.99,
        "tags": ["react", "frontend"],
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "isPublished": True
    }
]
db.courses.insert_many(courses)
print("Inserted 8 courses")

# Insert 15 enrollments
student_ids = [u["userId"] for u in db.users.find({"role": "student"})]
course_ids = [c["courseId"] for c in db.courses.find()]
enrollments = [
    {
        "userId": student_ids[i % len(student_ids)],
        "courseId": course_ids[i % len(course_ids)],
        "enrolledAt": datetime.now() - timedelta(days=i*10),
        "progress": round((i + 1) * 5.0, 2),  # 5% to 75%
        "completed": i % 3 == 0  # Some completed
    } for i in range(15)
]
db.enrollments.insert_many(enrollments)
print("Inserted 15 enrollments")

# Insert 25 lessons across courses
lessons = []
for i, course_id in enumerate(course_ids):
    for j in range(3 if i < 4 else 4):  # More lessons for some courses
        lessons.append({
            "lessonId": generate_id("L"),
            "courseId": course_id,
            "title": f"Lesson {j+1}: {['Intro', 'Core Concepts', 'Advanced Topics', 'Project'][j]}",
            "content": f"Content for lesson {j+1} in course {course_id}",
            "duration": 30.0 + j * 10,
            "createdAt": datetime.now()
        })
db.lessons.insert_many(lessons)
print("Inserted 25 lessons")

# Insert 10 assignments
assignments = []
for i, course_id in enumerate(course_ids[:5]):  # Limit to 5 courses
    for j in range(2):
        assignments.append({
            "assignmentId": generate_id("A"),
            "courseId": course_id,
            "title": f"Assignment {j+1}: {['Quiz', 'Project'][j]}",
            "description": f"Complete {['a quiz', 'a project'][j]} for course {course_id}",
            "dueDate": datetime.now() + timedelta(days=(j+1)*7),
            "maxScore": 100.0
        })
db.assignments.insert_many(assignments)
print("Inserted 10 assignments")

# Insert 12 submissions
assignment_ids = [a["assignmentId"] for a in db.assignments.find()]
submissions = [
    {
        "submissionId": generate_id("S"),
        "assignmentId": assignment_ids[i % len(assignment_ids)],
        "userId": student_ids[i % len(student_ids)],
        "content": f"Submission content for assignment {i+1}",
        "submittedAt": datetime.now() - timedelta(days=i*5),
        "grade": 70.0 + (i * 2),  # 70 to 94
        "feedback": f"Good work, improve on {['details', 'clarity'][i % 2]}"
    } for i in range(12)
]
db.submissions.insert_many(submissions)
print("Inserted 12 submissions")

# Task 2.2: Verify Relationships
# Check referential integrity
for course in db.courses.find():
    assert db.users.find_one({"userId": course["instructorId"]}), f"Invalid instructorId {course['instructorId']}"
for enrollment in db.enrollments.find():
    assert db.users.find_one({"userId": enrollment["userId"]}), f"Invalid userId {enrollment['userId']}"
    assert db.courses.find_one({"courseId": enrollment["courseId"]}), f"Invalid courseId {enrollment['courseId']}"
for lesson in db.lessons.find():
    assert db.courses.find_one({"courseId": lesson["courseId"]}), f"Invalid courseId {lesson['courseId']}"
for assignment in db.assignments.find():
    assert db.courses.find_one({"courseId": assignment["courseId"]}), f"Invalid courseId {assignment['courseId']}"
for submission in db.submissions.find():
    assert db.assignments.find_one({"assignmentId": submission["assignmentId"]}), f"Invalid assignmentId {submission['assignmentId']}"
    assert db.users.find_one({"userId": submission["userId"]}), f"Invalid userId {submission['userId']}"
print("Referential integrity verified")

# Export sample data to JSON
collections = ['users', 'courses', 'enrollments', 'lessons', 'assignments', 'submissions']
for collection in collections:
    data = list(db[collection].find())
    # Convert ObjectId and datetime to strings for JSON compatibility
    for doc in data:
        doc['_id'] = str(doc['_id'])
        for key, value in doc.items():
            if isinstance(value, datetime):
                doc[key] = value.isoformat()
    with open(f"data/{collection}.json", 'w') as f:
        json.dump(data, f, indent=2)
print("Exported sample data to data/*.json")