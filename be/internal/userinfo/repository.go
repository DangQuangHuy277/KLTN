package userinfo

import (
	"HNLP/be/internal/db"
)

type RepositoryImpl struct {
	db db.HDb
}

func NewUserInfoRepositoryImpl(db db.HDb) *RepositoryImpl {
	return &RepositoryImpl{db: db}
}

//func (r RepositoryImpl) GetUserInfo(ctx context.Context, req GetUserInfoRequest) (*GetUserInfoResponse, error){
//
//}
