package userinfo

import "context"

type Service interface {
	GetUserInfo(ctx context.Context, req GetUserInfoRequest) (*GetUserInfoResponse, error)
}

type Repository interface {
	GetUserInfo(ctx context.Context, req GetUserInfoRequest) (*GetUserInfoResponse, error)
}

type GetUserInfoRequest struct {
	UserID int     `json:"user_id"`
	Role   *string `json:"role"`
}

type GetUserInfoResponse struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Code     string `json:"code"`
	Birthday string `json:"birthday"`
	Email    string `json:"email"`
	AdministrativeClassID int `json:"administrative_class_id"`
}
