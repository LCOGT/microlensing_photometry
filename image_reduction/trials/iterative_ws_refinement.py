# breakpoint()
### might be valuable some looping here

# Refining???
# dists2 = distance.cdist(np.c_[sources['xcentroid'],sources['ycentroid']][:500],projected2[:500])
# mask2 = dists2<1
# lines2,cols2 = np.where(mask2)
# pts12 = np.c_[star_pix[0],star_pix[1]][:500][cols2]
# pts22 = np.c_[sources['xcentroid'],sources['ycentroid']][:500][lines2]

# model_robust2, inliers2 = ransac(( pts22,pts12), tf. AffineTransform,min_samples=10, residual_threshold=1, max_trials=300)
# projected22 = model_robust2.inverse(pts12)
# projected222 = model_robust2.inverse(np.c_[star_pix[0],star_pix[1]])

# print(shifts)
