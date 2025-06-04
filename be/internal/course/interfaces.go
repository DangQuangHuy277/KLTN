package course

import (
	"context"
)

type GetCourseRequest struct {
	Code *string `json:"code,omitempty"`
	Name *string `json:"name,omitempty"`
}

type CourseClass struct {
	ID         int      `json:"id" db:"id"`
	Code       string   `json:"code" db:"code"`
	CourseID   int      `json:"course_id" db:"course_id"`
	Semester   string   `json:"semester" db:"semester_id"`
	CourseCode string   `json:"course_code" db:"course_code"`
	CourseName string   `json:"course_name" db:"course_name"`
	GPA        *float64 `json:"gpa,omitempty" db:"gpa"`
}

type Service interface {
	GetCourse(ctx context.Context, req GetCourseRequest) (*GetCourseResponse, error)
}

type Repository interface {
	GetByCode(ctx context.Context, code string) (Course, error)
	GetByName(ctx context.Context, s string) (Course, error)
	GetByEnglishName(ctx context.Context, s string) (Course, error)
	GetAllCoursesOfStudentByID(ctx context.Context, id int) ([]CourseClass, error)
	GetAllCoursesOfStudentByName(ctx context.Context, studentName string) ([]CourseClass, error)
	GetStudentName(ctx context.Context, id int) (string, error)
	GetStudentByName(ctx context.Context, name string) (int, error)
}

type GetCourseResponse struct {
	Code        string `json:"code"`
	Name        string `json:"name"`
	EnglishName string `json:"english_name"`
}
