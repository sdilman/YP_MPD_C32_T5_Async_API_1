from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from services.film import FilmService, get_film_service

from models.movies import Film
from typing import List

router = APIRouter()



@router.get('/search', response_model=List[Film])
async def search_film(phrase: str,
                      page: int = Query(ge=1, default=1),
                      size: int = Query(ge=1, default=50),
                      film_service: FilmService = Depends(get_film_service)
    ):
    films = await film_service.get_by_search(phrase, page, size)
    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='films not found')
    return films

@router.get('/{film_id}', response_model=Film)
async def film_details(film_id: str, film_service: FilmService = Depends(get_film_service)) -> Film:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')
    return film


@router.get('/', response_model=List[Film])
async def film_details(
        sort: str = Query(None),
        genre: str = Query(None),
        query: str = Query(None),
        page: int = Query(ge=1, default=1),
        size: int = Query(ge=1, default=50),
        film_service: FilmService = Depends(get_film_service)
    ):
    films = await film_service.get(genre=genre, title=query, page=page, size=size)
    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='films not found')
    if sort:
        reverse_sort = False
        if sort.startswith('-'):
            reverse_sort = True
            sort = sort[1:]  # Убираем знак минуса
        films = sorted(films, key=lambda x: getattr(x, sort), reverse=reverse_sort)
    return films




