package courseclass

import (
	"HNLP/be/internal/course"
	"context"
)

type MockCourseService struct {
}

func NewMockCourseService() *MockCourseService {
	return &MockCourseService{}
}

func (m *MockCourseService) GetCourse(ctx context.Context, req course.GetCourseRequest) (*course.GetCourseResponse, error) {
	return &course.GetCourseResponse{
		Code:        "CS101",
		Name:        "Computer Science 101",
		EnglishName: "Computer Science 101",
	}, nil
}
