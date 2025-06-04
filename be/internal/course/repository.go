package course

import (
	"HNLP/be/internal/db"
	"context"
)

type RepositoryImpl struct {
	db db.HDb
}

func NewRepositoryImpl(db db.HDb) *RepositoryImpl {
	return &RepositoryImpl{db: db}
}

func (r *RepositoryImpl) GetByCode(ctx context.Context, code string) (Course, error) {
	var course Course
	err := r.db.GetContext(ctx, &course, "SELECT * FROM course WHERE code = $1", code)
	return course, err
}

func (r *RepositoryImpl) GetByName(ctx context.Context, name string) (Course, error) {
	var course Course
	err := r.db.GetContext(ctx, &course, "SELECT * FROM course WHERE name = $1", name)
	return course, err
}

func (r *RepositoryImpl) GetAllCoursesOfStudentByID(ctx context.Context, id int) ([]CourseClass, error) {
	var courseClass []CourseClass
	err := r.db.SelectContext(ctx, &courseClass, `select cc.id, cc.code, cc.semester_id, c.code as course_code, c.name as course_name, cce.gpa as gpa
from course_class_enrollment cce
         join course_class cc on cc.id = cce.course_class_id
         join course c on c.id = cc.course_id
where cce.student_id = $1;`, id)
	return courseClass, err
}

func (r *RepositoryImpl) GetAllCoursesOfStudentByName(ctx context.Context, studentName string) ([]CourseClass, error) {
	var courseClass []CourseClass
	err := r.db.SelectContext(ctx, &courseClass, `select cc.id, cc.code, cc.semester_id, c.code as course_code, c.name as course_name, cce.gpa as gpa
from course_class_enrollment cce
         join course_class cc on cc.id = cce.course_class_id
         join course c on c.id = cc.course_id
		 join student s on s.id = cce.student_id
where s.name = $1`, studentName)
	if err != nil {
		return nil, err
	}
	return courseClass, nil
}

func (r *RepositoryImpl) GetStudentName(ctx context.Context, id int) (string, error) {
	var name string
	err := r.db.GetContext(ctx, &name, "SELECT name FROM student WHERE id = $1", id)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (r *RepositoryImpl) GetStudentByName(ctx context.Context, name string) (int, error) {
	var id int
	err := r.db.GetContext(ctx, &id, "SELECT id FROM student WHERE name = $1", name)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *RepositoryImpl) GetByEnglishName(ctx context.Context, englishName string) (Course, error) {
	var course Course
	err := r.db.GetContext(ctx, &course, "SELECT * FROM course WHERE english_name = $1", englishName)
	return course, err
}
