package course

import (
	"HNLP/be/internal/db"
	"context"
	"errors"
)

type ServiceImpl struct {
	repo Repository
	db   *db.SQLHDb
}

func NewServiceImpl(repo Repository, db *db.SQLHDb) *ServiceImpl {
	return &ServiceImpl{
		repo: repo,
		db:   db,
	}
}

func (s *ServiceImpl) GetCourse(ctx context.Context, req GetCourseRequest) (*GetCourseResponse, error) {
	var course Course
	var err error

	// Try ID first if provided
	if req.Code != nil {
		course, err = s.repo.GetByCode(ctx, *req.Code)
		if err == nil {
			return &GetCourseResponse{
				Code:        course.Code,
				Name:        course.Name,
				EnglishName: course.EnglishName,
			}, nil
		}
	}

	// Try name if provided (either as fallback from ID or direct request)
	if req.Name != nil {
		// Try regular name
		course, err = s.repo.GetByName(ctx, *req.Name)
		if err == nil {
			return &GetCourseResponse{
				Code:        course.Code,
				Name:        course.Name,
				EnglishName: course.EnglishName,
			}, nil
		}

		// Try English name
		course, err = s.repo.GetByEnglishName(ctx, *req.Name)
		if err == nil {
			return &GetCourseResponse{
				Code:        course.Code,
				Name:        course.Name,
				EnglishName: course.EnglishName,
			}, nil
		}
	}

	// If we reach here, no course was found
	return nil, errors.New("course not found")
}

type GetGpaOfStudentRequest struct {
	StudentId   *int    `json:"student_id"`
	StudentName *string `json:"student_name"`
}

type GetGpaOfStudentResponse struct {
	GPA float64 `json:"gpa"`
}

func (s *ServiceImpl) GetCurrentGpaOfStudent(ctx context.Context, req GetGpaOfStudentRequest) (GetGpaOfStudentResponse, error) {
	// Get all courses of the student
	role := ctx.Value("userRole").(string)
	specificId := ctx.Value("specificId").(int)
	if role == "student" {
		if req.StudentId != nil && (*req.StudentId != specificId || *req.StudentId == 0) {
			studentName, _ := s.repo.GetStudentName(ctx, specificId)
			if req.StudentName != nil && *req.StudentName != studentName {
				return GetGpaOfStudentResponse{}, errors.New("you are not allowed to get GPA of this student")
			}
		}
	} else if role == "professor" {
		professorInfo, _ := s.db.FetchProfessorInfo(ctx, specificId)
		var studentId int
		if req.StudentId != nil && *req.StudentId != 0 {
			studentId = *req.StudentId
		} else {
			studentId, _ = s.repo.GetStudentByName(ctx, *req.StudentName)
		}
		if !db.ContainsInt(professorInfo.TaughtStudentIDs, studentId) && !db.ContainsInt(professorInfo.AdvisedStudentIDs, studentId) {
			return GetGpaOfStudentResponse{}, errors.New("Bạn không có quyền truy cập dữ liệu của sinh viên này")
		}
	}

	var courseClasses []CourseClass
	var err error
	if req.StudentId != nil && *req.StudentId != 0 {
		courseClasses, err = s.repo.GetAllCoursesOfStudentByID(ctx, *req.StudentId)
	} else if req.StudentName != nil {
		courseClasses, err = s.repo.GetAllCoursesOfStudentByName(ctx, *req.StudentName)
	} else {
		return GetGpaOfStudentResponse{}, errors.New("student id or name is required")
	}

	if err != nil {
		return GetGpaOfStudentResponse{}, err
	}
	gpa := 0.0
	count := 0
	for _, courseClass := range courseClasses {
		if courseClass.GPA != nil {
			gpa += *courseClass.GPA
			count++
		}
	}
	if count == 0 {
		return GetGpaOfStudentResponse{}, errors.New("không tìm thấy dữ liệu điểm hoặc không có quyền truy cập dữ liệu của sinh viên này")
	}

	return GetGpaOfStudentResponse{
		GPA: gpa / float64(count),
	}, nil
}
