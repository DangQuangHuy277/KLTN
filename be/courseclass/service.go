package courseclass

import (
	"HNLP/be/internal/course"
	"HNLP/be/internal/db"
)

import (
	"context"
	"errors"
)

type ServiceImpl struct {
	courseService course.Service
	repo          Repository
	authService   db.AuthorizationServiceImpl
}

func NewServiceImpl(repo Repository, courseService course.Service) *ServiceImpl {
	return &ServiceImpl{
		repo:          repo,
		courseService: courseService,
	}
}

func (s *ServiceImpl) GetCourseClass(ctx context.Context, req GetCourseClassRequest) (*GetCourseClassResponse, error) {
	var courseClass CourseClass
	var err error
	// Get User ID from context
	userID := ctx.Value("userID").(int)
	if userID == 0 {
		return nil, errors.New("user not found")
	}
	role := ctx.Value("role").(string)
	if role == "" {
		return nil, errors.New("role not found")
	}

	// Try ID first if provided
	if req.Code != nil {
		courseClass, err = s.repo.GetByCode(ctx, *req.Code)

		if role == "student" {
		}

		if err == nil {
			return &GetCourseClassResponse{
				CourseClass: courseClass,
			}, nil
		}
	}

	// If we reach here, no course class was found
	return nil, errors.New("course class not found")
}
