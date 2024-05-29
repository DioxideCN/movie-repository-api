type MetadataType = {
    clip_id?: string;
    views?: string;
    update_time?: string;
    batch?: number;
    batch_order?: number;
    entity_id?: number;
    album_id?: number;
    tv_id?: number;
    summary?: {
        summaryType: string;
        type: string;
        img: string;
        summary: string;
        title: string;
        subTitle: string;
        videoLink: string;
        rightTagText: string;
        rightTagColor: string;
    };
    id?: string;
    order?: string;
    url?: string;
};

type PlatformDetailType = {
    source: string;
    title: string;
    cover_url: string;
    create_time: string;
    directors: string[];
    actors: string[];
    release_date: string;
    description: string;
    score: number;
    movie_type: string[];
    metadata: MetadataType;
};

type MovieType = {
    _id: string;
    fixed_title: string;
    create_time: string;
    update_time: string;
    platform_detail: PlatformDetailType[];
};