package user

type User struct {
	ID          int    `json:"id"`
	Realname    string `json:"realname"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	Role        Role   `json:"role"`
	Disabled    bool   `json:"disabled"`
	StudentID   *int   `json:"student_id" db:"student_id"`
	ProfessorID *int   `json:"professor_id" db:"professor_id"`
}
