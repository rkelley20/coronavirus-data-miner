import pandemics.repo
import pandemics.processing
import pandemics.utils
import pandemics.fetch

if __name__ == '__main__':
    # Clone repo and get JHU data
    pandemics.repo.clone_jhu(force=False)
    recovered_jhu = pandemics.repo.jhu_recovered(normalize=True)
    confirmed_jhu = pandemics.repo.jhu_confirmed(normalize=True)
    deaths_jhu = pandemics.repo.jhu_deaths(normalize=True)

    # Get the most recent world data (contains confirmed, deaths, and recovered all in one)
    unh_world = pandemics.fetch.world_data(normalize=True)

    recovered_unh, confirmed_unh, deaths_unh = pandemics.processing.split_data(unh_world)

    recovered = pandemics.processing.join_unh_jhu(recovered_jhu, recovered_unh, greatest=True)
    confirmed = pandemics.processing.join_unh_jhu(confirmed_jhu, confirmed_unh, greatest=True)
    deaths = pandemics.processing.join_unh_jhu(deaths_jhu, deaths_unh, greatest=True)










