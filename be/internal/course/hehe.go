package course

import "context"

type CourseClassService interface {
	GetCourseClassInfo(ctx context.Context, req GetCourseRequest) (*GetCourseResponse, error)
}

type CourseClassServiceImpl struct {
	repo     Repository
	gradeSrv GradeService
}

func NewCourseClassServiceImpl(gradeSrv GradeService, repo Repository) *CourseClassServiceImpl {
	return &CourseClassServiceImpl{
		repo:     repo,
		gradeSrv: gradeSrv,
	}
}

type GradeService interface {
	GetGpaOfStudent(ctx context.Context, req GetGpaOfStudentRequest) (*GetGpaOfStudentResponse, error)
}
type MockGradeService struct {
}

func NewMockCourseService() *MockGradeService {
	return &MockGradeService{}
}

func (g *MockGradeService) GetGpaOfStudent(ctx context.Context, req GetGpaOfStudentRequest) (*GetGpaOfStudentResponse, error) {
	return &GetGpaOfStudentResponse{
		GPA: 3.5,
	}, nil
}
