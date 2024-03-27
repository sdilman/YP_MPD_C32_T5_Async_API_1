from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from models.movies import Genre
from services.genre import GenreService, get_genre_service

router = APIRouter()


@router.get('/{genre_id}', response_model=Genre)
async def genre_details(genre_id: str, genre_service: GenreService = Depends(get_genre_service)):
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')
    return genre


@router.get('/', response_model=list[Genre])
async def all_genres(genre_service: GenreService = Depends(get_genre_service)):
    genres = await genre_service.get_all()
    if not genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genres not found')
    return genres
