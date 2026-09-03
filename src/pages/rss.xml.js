import { getCollection, render } from 'astro:content';
import rss from '@astrojs/rss';
import { experimental_AstroContainer as AstroContainer } from 'astro/container';
import { SITE_DESCRIPTION, SITE_TITLE } from '../consts';

export async function GET(context) {
	const posts = await getCollection('blog');
	const container = await AstroContainer.create();

	const items = await Promise.all(
		posts.map(async (post) => {
			const { Content } = await render(post);
			const html = await container.renderToString(Content);
			return {
				...post.data,
				link: `/blog/${post.id}/`,
				content: html,
			};
		}),
	);

	return rss({
		title: SITE_TITLE,
		description: SITE_DESCRIPTION,
		site: context.site,
		items,
	});
}