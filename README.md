# mongodb-eduhub-project
AltSchool second semester project
# EduHub MongoDB Project
This project implements a MongoDB database for "EduHub," an e-learning platform, as part of the AltSchool of Data Engineering Tinyuka 2024 Second Semester Exam. It demonstrates MongoDB fundamentals, including schema design, CRUD operations, aggregations, indexing, and performance optimization.

## Project Overview
- **Database**: MongoDB v8.0+
- **Tools**: Python (PyMongo), MongoDB Compass, Jupyter Notebook
- **Collections**: users, courses, enrollments, lessons, assignments, submissions
- **Features**: User management, course creation, enrollment tracking, assignment grading, analytics

## Setup Instructions
1. Install MongoDB locally or use MongoDB Atlas.
2. Install Python dependencies: `pip install pymongo pandas`.
3. Clone this repository: `git clone https://github.com/Daniel-ok2/mongodb-eduhub-project.git`.
4. Update the MongoDB connection string in `notebooks/eduhub_mongodb_project.ipynb`.
5. Run the Jupyter Notebook to set up the database and execute operations.

## Repository Structure
mongodb-eduhub-project/
├── README.md
├── notebooks/
│   └── eduhub_mongodb_project.ipynb
├── src/
│   └── eduhub_queries.py
├── data/
│   ├── sample_data.json
│   └── schema_validation.json
├── docs/
│   ├── performance_analysis.md
│   └── presentation.pptx
└── .gitignore
          
           Database Schema
           
The database (eduhub_db) includes six collections:

users: Stores student/instructor details (userId, email, role, profile).
courses: Manages course metadata (courseId, title, instructorId, price).
enrollments: Tracks student-course relationships (userId, courseId, progress).
lessons: Contains lesson content (lessonId, courseId, title).
assignments: Defines assignments (assignmentId, courseId, dueDate).
submissions: Records submissions (submissionId, assignmentId, grade).
See data/schema_validation.json for detailed schema definitions.
