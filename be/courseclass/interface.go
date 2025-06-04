package courseclass

import "context"

type CourseClass struct {
	ID         int    `json:"id" db:"id"`
	Code       string `json:"code" db:"code"`
	CourseID   int    `json:"course_id" db:"course_id"`
	SemesterID int    `json:"semester_id" db:"semester_id"`
}

type Repository interface {
	GetByCode(ctx context.Context, code string) (CourseClass, error)
}

type GetCourseClassRequest struct {
	Code *string `json:"code,omitempty"`
}

type GetCourseClassResponse struct {
	CourseClass CourseClass `json:"course_class"`
}

type Service interface {
	GetCourseClass(ctx context.Context, req GetCourseClassRequest) (*GetCourseClassResponse, error)
}
